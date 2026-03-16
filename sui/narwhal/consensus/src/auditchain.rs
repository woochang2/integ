// Copyright (c) 2025, AuditChain contributors
// SPDX-License-Identifier: Apache-2.0

//! AuditChain consensus with two cut strategies:
//!
//! 1. **Relaxed Mempool cut** (`inclusive = false`):
//!    Select the top 2f+1 authorities by DAG height and compute H_min = min(their heights).
//!    Commit all certificates up to H_min. Slower validators are excluded from the cut.
//!
//! 2. **Autobahn-style inclusive cut** (`inclusive = true`):
//!    Include ALL n authorities. For each authority, take their latest certified height.
//!    Commit all certificates up to each authority's own tip. No validator is excluded.
//!    Requires at least n-f authorities to have new progress (lane coverage).

use crate::{
    consensus::{ConsensusProtocol, ConsensusState, Dag},
    ConsensusError, Outcome,
};
use config::{Committee, Stake};
use crypto::PublicKey;
use fastcrypto::hash::Hash;
use fastcrypto::traits::EncodeDecodeBase64;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};
use types::{Certificate, CommittedSubDag, ConsensusStore, Round};

pub struct AuditChain {
    pub committee: Committee,
    pub store: Arc<ConsensusStore>,
    pub gc_depth: Round,
    /// If true, use Autobahn-style inclusive cut (all n lanes).
    /// If false, use Relaxed Mempool cut (top 2f+1, H_min).
    pub inclusive: bool,
}

impl AuditChain {
    pub fn new(
        committee: Committee,
        store: Arc<ConsensusStore>,
        gc_depth: Round,
        inclusive: bool,
    ) -> Self {
        info!(
            "AuditChain created with {} cut strategy",
            if inclusive { "inclusive (Autobahn-style)" } else { "conservative (Relaxed Mempool)" }
        );
        Self {
            committee,
            store,
            gc_depth,
            inclusive,
        }
    }

    /// Find each authority's latest round in the DAG.
    fn latest_heights<'a>(dag: &'a Dag) -> HashMap<&'a PublicKey, Round> {
        let mut latest: HashMap<&PublicKey, Round> = HashMap::new();
        for (round, by_author) in dag.iter() {
            for (author, _) in by_author.iter() {
                let entry = latest.entry(author).or_insert(0);
                if *round > *entry {
                    *entry = *round;
                }
            }
        }
        latest
    }

    /// Relaxed Mempool cut: top 2f+1 by height, H_min = min of those.
    fn compute_relaxed_cut(&self, dag: &Dag) -> Option<Round> {
        let latest = Self::latest_heights(dag);
        if latest.is_empty() {
            return None;
        }

        let mut entries: Vec<(&PublicKey, Round)> = latest.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1)); // descending by height

        // Use quorum_threshold (2f+1) for the Relaxed Mempool paper spec.
        let quorum: Stake = self.committee.quorum_threshold();
        let mut acc_stake: Stake = 0;
        let mut h_min = Round::MAX;

        for (pk, r) in &entries {
            acc_stake += self.committee.stake(pk);
            if *r < h_min {
                h_min = *r;
            }
            if acc_stake >= quorum {
                break;
            }
        }

        if acc_stake < quorum {
            return None;
        }

        Some(h_min)
    }

    /// Autobahn-style inclusive cut: include all n authorities' latest tips.
    /// Returns the maximum round across all authorities (commit up to each authority's own tip).
    /// Requires at least n-f authorities to have progress beyond last_committed_round.
    fn compute_inclusive_cut(&self, dag: &Dag, last_committed_round: Round) -> Option<Round> {
        let latest = Self::latest_heights(dag);
        if latest.is_empty() {
            return None;
        }

        // Lane coverage: count how many authorities have new progress
        let n_f_threshold: Stake = self.committee.quorum_threshold(); // 2f+1
        let mut coverage_stake: Stake = 0;
        let mut max_round: Round = 0;

        for (pk, r) in &latest {
            if *r > last_committed_round {
                coverage_stake += self.committee.stake(pk);
            }
            if *r > max_round {
                max_round = *r;
            }
        }

        // Need at least n-f (= 2f+1) authorities with new tips for lane coverage
        if coverage_stake < n_f_threshold {
            return None;
        }

        // In inclusive mode, we commit up to the maximum round seen.
        // All authorities' certificates up to their own latest height are included.
        Some(max_round)
    }

    /// Collect all certificates up to round <= cut_round, sorted deterministically.
    fn collect_prefix(dag: &Dag, cut_round: Round) -> Vec<Certificate> {
        let mut seq = Vec::new();
        for (_round, by_author) in dag.range(..=cut_round) {
            for (_author, (_digest, cert)) in by_author.iter() {
                seq.push(cert.clone());
            }
        }
        seq.sort_by(|a, b| {
            let ra = a.round();
            let rb = b.round();
            if ra != rb {
                return ra.cmp(&rb);
            }
            a.origin().encode_base64().cmp(&b.origin().encode_base64())
        });
        seq
    }

    fn pick_leader_for_subdag(
        committee: &Committee,
        dag: &Dag,
        cut_round: Round,
    ) -> Option<Certificate> {
        let leader_pk = committee.leader(cut_round);
        // Try exact round first
        if let Some(by_author) = dag.get(&cut_round) {
            if let Some((_digest, cert)) = by_author.get(&leader_pk) {
                return Some(cert.clone());
            }
        }
        // Fallback: leader at or below cut_round
        for r in (0..=cut_round).rev() {
            if let Some(by_author) = dag.get(&r) {
                if let Some((_digest, cert)) = by_author.get(&leader_pk) {
                    return Some(cert.clone());
                }
            }
        }
        // Last fallback: any cert at or below cut_round
        for r in (0..=cut_round).rev() {
            if let Some(by_author) = dag.get(&r) {
                if let Some((_pk, (_digest, cert))) = by_author.iter().next() {
                    return Some(cert.clone());
                }
            }
        }
        None
    }
}

impl ConsensusProtocol for AuditChain {
    fn process_certificate(
        &mut self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        debug!("AuditChain processing {:?}", certificate);
        let round = certificate.round();

        // 1) Insert the certificate into the DAG.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // 2) Compute the cut based on the selected strategy.
        let cut_round = if self.inclusive {
            self.compute_inclusive_cut(&state.dag, state.last_committed_round)
        } else {
            self.compute_relaxed_cut(&state.dag)
        };

        let cut_round = match cut_round {
            Some(r) => r,
            None => return Ok((Outcome::NotEnoughSupportForLeader, Vec::new())),
        };

        // Skip if we've already committed beyond this round.
        if cut_round <= state.last_committed_round {
            return Ok((Outcome::LeaderBelowCommitRound, Vec::new()));
        }

        // 3) Collect all certificates up to cut_round.
        let sequence = Self::collect_prefix(&state.dag, cut_round);
        if sequence.is_empty() {
            return Ok((Outcome::LeaderNotFound, Vec::new()));
        }

        // 4) Update state (GC-aware).
        for c in &sequence {
            state.update(c, self.gc_depth);
        }

        // 5) Pick a leader certificate.
        let leader_cert = Self::pick_leader_for_subdag(&self.committee, &state.dag, cut_round)
            .unwrap_or_else(|| sequence.last().cloned().unwrap());

        // 6) Package and persist.
        let next_sub_dag_index = state.latest_sub_dag_index + 1;
        let sub_dag = CommittedSubDag {
            certificates: sequence,
            leader: leader_cert,
            sub_dag_index: next_sub_dag_index,
        };

        self.store
            .write_consensus_state(&state.last_committed, &sub_dag)?;
        state.latest_sub_dag_index = next_sub_dag_index;

        debug!(
            "AuditChain commit at round {} (inclusive={}) sub_dag_index={}",
            cut_round, self.inclusive, next_sub_dag_index
        );

        Ok((Outcome::Commit, vec![sub_dag]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::ConsensusMetrics;
    use prometheus::Registry;
    use std::collections::BTreeSet;
    use test_utils::{make_consensus_store, CommitteeFixture};

    #[tokio::test]
    async fn relaxed_cut_commit() {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let keys: Vec<_> = fixture.authorities().map(|a| a.public_key()).collect();
        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();

        let rounds: Round = 12;
        let (certs, _) =
            test_utils::make_optimal_certificates(&committee, 1..=rounds, &genesis, &keys);

        let store = make_consensus_store(&test_utils::temp_dir());
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        let mut state = ConsensusState::new(metrics);
        let mut ac = AuditChain::new(committee, store, 12, false);

        for c in certs {
            let _ = ac.process_certificate(&mut state, c);
        }
        assert!(state.last_committed_round > 0);
    }

    #[tokio::test]
    async fn inclusive_cut_commit() {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let keys: Vec<_> = fixture.authorities().map(|a| a.public_key()).collect();
        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();

        let rounds: Round = 12;
        let (certs, _) =
            test_utils::make_optimal_certificates(&committee, 1..=rounds, &genesis, &keys);

        let store = make_consensus_store(&test_utils::temp_dir());
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        let mut state = ConsensusState::new(metrics);
        let mut ac = AuditChain::new(committee, store, 12, true);

        for c in certs {
            let _ = ac.process_certificate(&mut state, c);
        }
        assert!(state.last_committed_round > 0);
    }
}
