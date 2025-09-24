// Copyright (c) 2025, AuditChain contributors
// SPDX-License-Identifier: Apache-2.0

//! AuditChain consensus: cut a safe prefix of the DAG using height lists semantics,
//! then finalize via a two-step (Prepare→Commit) flow. This adapts cleanly to the
//! existing ConsensusProtocol trait used by Tusk.
//
//  High-level (matching the attached spec):
//  - Maintain the DAG as usual (ConsensusState.dag).
//  - Upon each new certificate, compute a "safe cut":
//      * Find each authority's latest known certificate height (round).
//      * Select the top 2f+1 authorities by height and take H_min = min(their heights).
//      * Commit ALL certificates up to round H_min across all authorities.
//  - Package the committed certificates into a single CommittedSubDag and persist.
//  - If quorum not sufficient yet, return a non-commit Outcome.
//
//  Notes:
//  - We do not explicitly model separate network messages (Prepare/Commit) here since
//    this engine is driven by incoming certificates; instead, we directly produce a
//    new committed sub-DAG whenever the cut is safe. This mirrors the “leader proposes
//    a cut, replicas agree” semantics in a single local step.
//  - This file intentionally mirrors the structure of tusk.rs for minimal integration.

use crate::{
    consensus::{ConsensusProtocol, ConsensusState, Dag},
    ConsensusError, Outcome,
};
use config::{Committee, Stake};
use fastcrypto::traits::EncodeDecodeBase64;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::debug;
use types::{Certificate, CertificateDigest, CommittedSubDag, ConsensusStore, PublicKey, Round};

pub struct AuditChain {
    /// Committee info (stake, thresholds).
    pub committee: Committee,
    /// Persistent storage for crash recovery.
    pub store: Arc<ConsensusStore>,
    /// GC depth (how many rounds behind we retain).
    pub gc_depth: Round,
}

impl AuditChain {
    pub fn new(committee: Committee, store: Arc<ConsensusStore>, gc_depth: Round) -> Self {
        Self {
            committee,
            store,
            gc_depth,
        }
    }

    /// Compute the "safe cut" H_min using the committee's validity threshold (≈ 2f+1).
    ///
    /// Returns (Some(H_min), top_group) if we have enough live streams; otherwise None.
    /// - latest_by_author: (pk -> (round, digest, cert))
    fn compute_safe_cut<'a>(
        &self,
        dag: &'a Dag,
    ) -> Option<(Round, Vec<(&'a PublicKey, Round)>)> {
        // 1) Find each authority’s latest round present in the DAG.
        let mut latest_by_author: HashMap<&PublicKey, (Round, &CertificateDigest)> = HashMap::new();

        // Iterate rounds ascending; the latest will overwrite older entries.
        for (round, by_author) in dag.iter() {
            for (author, (digest, _cert)) in by_author.iter() {
                match latest_by_author.get(author) {
                    Some((existing_r, _)) if *existing_r >= *round => {}
                    _ => {
                        latest_by_author.insert(author, (*round, digest));
                    }
                }
            }
        }

        if latest_by_author.is_empty() {
            return None;
        }

        // 2) Sort authorities by latest height desc, pick the top quorum (validity_threshold).
        let mut entries: Vec<(&PublicKey, Round)> = latest_by_author
            .iter()
            .map(|(pk, (r, _d))| (*pk, *r))
            .collect();

        // Highest progress first.
        entries.sort_by(|a, b| b.1.cmp(&a.1));

        // The threshold used by Tusk code for "enough support".
        let quorum: Stake = self.committee.validity_threshold();

        // Compute how many authorities we need to reach 'quorum' stake.
        // We greedily accumulate from the top.
        let mut acc_stake: Stake = 0;
        let mut cut_group: Vec<(&PublicKey, Round)> = Vec::new();
        for (pk, r) in entries.iter() {
            acc_stake += self.committee.stake(pk);
            cut_group.push((*pk, *r));
            if acc_stake >= quorum {
                break;
            }
        }

        if acc_stake < quorum {
            return None;
        }

        // 3) Safe cut is the MIN round among the selected top stake group.
        let h_min = cut_group.iter().map(|(_, r)| *r).min().unwrap_or(0);
        Some((h_min, cut_group))
    }

    /// Collect all certificates up to round <= H_min (inclusive), sorted by (round asc, author).
    fn collect_prefix(
        dag: &Dag,
        h_min: Round,
    ) -> Vec<Certificate> {
        let mut seq = Vec::new();
        for (round, by_author) in dag.range(..=h_min) {
            for (_author, (_digest, cert)) in by_author.iter() {
                seq.push(cert.clone());
            }
        }
        // Order by (round asc, author base64) for determinism.
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

    /// Pick a leader certificate to tag the CommittedSubDag.
    /// Prefer the committee’s leader for H_min if present up to that round; else fall back to any.
    fn pick_leader_for_subdag(
        committee: &Committee,
        dag: &Dag,
        h_min: Round,
    ) -> Option<Certificate> {
        let leader_pk = committee.leader(h_min);
        if let Some(by_author) = dag.get(&h_min) {
            if let Some((_d, c)) = by_author.get(leader_pk) {
                return Some(c.clone());
            }
        }
        // Fallback: find the latest cert for the leader at or below h_min.
        for r in (0..=h_min).rev() {
            if let Some(by_author) = dag.get(&r) {
                if let Some((_d, c)) = by_author.get(leader_pk) {
                    return Some(c.clone());
                }
            }
        }
        // Last fallback: any latest cert at or below h_min.
        for r in (0..=h_min).rev() {
            if let Some(by_author) = dag.get(&r) {
                if let Some((_d, c)) = by_author.iter().next() {
                    return Some(c.clone());
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

        // 1) Insert the certificate into the in-memory DAG.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // 2) Compute the safe DAG cut (H_min) using top 2f+1 (stake) streams.
        let (h_min, _top_group) = match self.compute_safe_cut(&state.dag) {
            Some(x) => x,
            None => {
                // Not enough visibility / stake yet; nothing to commit this time.
                return Ok((Outcome::NotEnoughSupportForLeader, Vec::new()));
            }
        };

        // If we've already committed beyond or equal to h_min, no new work.
        if h_min <= state.last_committed_round {
            return Ok((Outcome::LeaderBelowCommitRound, Vec::new()));
        }

        // 3) Build the commit prefix: all certs with round <= H_min.
        let mut sequence = Self::collect_prefix(&state.dag, h_min);

        if sequence.is_empty() {
            return Ok((Outcome::LeaderNotFound, Vec::new()));
        }

        // 4) Apply GC-aware state updates in commit order.
        for c in &sequence {
            state.update(c, self.gc_depth);
        }

        // 5) Pick a representative leader certificate for this sub-dag.
        let leader_cert = match Self::pick_leader_for_subdag(&self.committee, &state.dag, h_min) {
            Some(c) => c,
            None => {
                // Fallback: use the last element of the committed sequence.
                sequence
                    .last()
                    .cloned()
                    .ok_or_else(|| ConsensusError::ShuttingDown)?
            }
        };

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

        // Log last-commit rounds per authority (debug).
        for (name, round) in &state.last_committed {
            debug!("Latest commit of {}: Round {}", name.encode_base64(), round);
        }

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
    async fn basic_cut_and_commit() {
        let gc_depth: Round = 12;

        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let keys: Vec<_> = fixture.authorities().map(|a| a.public_key()).collect();

        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();

        // Build a small set of "nice" certificates.
        let rounds: Round = 12;
        let (certs, _next_parents) =
            test_utils::make_optimal_certificates(&committee, 1..=rounds, &genesis, &keys);

        let store_path = test_utils::temp_dir();
        let store = make_consensus_store(&store_path);
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

        let mut state = ConsensusState::new(metrics);
        let mut ac = AuditChain::new(committee, store, gc_depth);

        for c in certs {
            let _ = ac.process_certificate(&mut state, c);
        }

        // We should have advanced last_committed_round close to `rounds`.
        assert!(state.last_committed_round > 0);
    }
}
