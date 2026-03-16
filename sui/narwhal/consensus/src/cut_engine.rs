// Copyright (c) 2025, POSTECH System Software Lab
// SPDX-License-Identifier: Apache-2.0

//! Cut-based consensus engines for DAG-based BFT mempools.
//!
//! This module implements three cut strategies as ConsensusProtocol:
//!
//! 1. **Relaxed Mempool cut** (`CutStyle::Relaxed`):
//!    The Relaxed Mempool's leader-driven DAG cut algorithm.
//!    Selects the top 2f+1 validators by DAG height, computes H_min = min(their heights),
//!    and commits all certificates up to H_min. Bottom f validators are excluded from the cut,
//!    preventing stragglers from delaying commits.
//!
//! 2. **Autobahn-style inclusive cut** (`CutStyle::Inclusive`):
//!    Inspired by Autobahn's lane-based design. Includes ALL n validators' latest tips
//!    in the cut, regardless of their progress. Requires lane coverage (n-f new tips).
//!    After commit, replicas sync missing data. No validator is excluded.
//!
//! 3. **Adaptive cut** (`CutStyle::Adaptive`):
//!    Dynamically switches between Relaxed and Inclusive modes based on real-time
//!    height disparity across validators. When disparity is high (stragglers detected),
//!    uses Relaxed cut. When disparity is low or recovering from a blip, uses Inclusive cut.
//!
//! NOTE: These engines handle the consensus ordering/commit logic only.
//! The Relaxed Mempool's mempool-layer changes (f+1 weak quorum for block creation,
//! stream height lists, per-validator streams without global rounds) are implemented
//! separately in the primary/worker layers. AuditChain (the pipelined SMR backend)
//! is a separate component in the executor layer.

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

/// Which cut strategy to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutStyle {
    /// Relaxed Mempool: top 2f+1, uniform H_min.
    Relaxed,
    /// Autobahn-style: all n lane tips, per-lane cut.
    Inclusive,
    /// Adaptive: switches between Relaxed and Inclusive based on height disparity.
    Adaptive,
}

/// Height disparity threshold for the adaptive cut.
/// When max_height - min_height > this value, switch to Relaxed (straggler mode).
/// When <= this value, use Inclusive (for faster blip recovery).
const ADAPTIVE_DISPARITY_THRESHOLD: Round = 4;

pub struct CutEngine {
    pub committee: Committee,
    pub store: Arc<ConsensusStore>,
    pub gc_depth: Round,
    pub style: CutStyle,
    /// Tracks which mode the adaptive engine is currently using.
    adaptive_current_mode: CutStyle,
}

impl CutEngine {
    pub fn new(
        committee: Committee,
        store: Arc<ConsensusStore>,
        gc_depth: Round,
        style: CutStyle,
    ) -> Self {
        info!("CutEngine created with {:?} strategy", style);
        Self {
            committee,
            store,
            gc_depth,
            style,
            adaptive_current_mode: CutStyle::Relaxed, // start conservative
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

    /// Compute height disparity: max_height - min_height among validators present in DAG.
    fn height_disparity(heights: &HashMap<&PublicKey, Round>) -> Round {
        if heights.is_empty() {
            return 0;
        }
        let max_h = heights.values().copied().max().unwrap_or(0);
        let min_h = heights.values().copied().min().unwrap_or(0);
        max_h.saturating_sub(min_h)
    }

    /// Relaxed Mempool cut: top 2f+1 by height, H_min = min of those.
    /// This is the conservative cut from the Relaxed Mempool paper.
    fn compute_relaxed_cut(&self, dag: &Dag) -> Option<Round> {
        let latest = Self::latest_heights(dag);
        if latest.is_empty() {
            return None;
        }

        let mut entries: Vec<(&PublicKey, Round)> = latest.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1)); // descending by height

        let quorum: Stake = self.committee.quorum_threshold(); // 2f+1
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
    /// Commits up to the maximum round seen across all authorities.
    /// Requires lane coverage: at least n-f (2f+1) authorities with new progress.
    fn compute_inclusive_cut(&self, dag: &Dag, last_committed_round: Round) -> Option<Round> {
        let latest = Self::latest_heights(dag);
        if latest.is_empty() {
            return None;
        }

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

        if coverage_stake < n_f_threshold {
            return None;
        }

        Some(max_round)
    }

    /// Adaptive cut: decides between Relaxed and Inclusive based on height disparity.
    ///
    /// Strategy:
    /// - Compute height disparity among all validators in the DAG.
    /// - If disparity > ADAPTIVE_DISPARITY_THRESHOLD → stragglers detected → use Relaxed cut
    ///   (exclude slow nodes to maintain low latency).
    /// - If disparity <= threshold → nodes are in sync or recovering from blip → use Inclusive cut
    ///   (commit all backlog at once for fast recovery).
    fn compute_adaptive_cut(&mut self, dag: &Dag, last_committed_round: Round) -> Option<Round> {
        let latest = Self::latest_heights(dag);
        let disparity = Self::height_disparity(&latest);

        let chosen_mode = if disparity > ADAPTIVE_DISPARITY_THRESHOLD {
            CutStyle::Relaxed
        } else {
            CutStyle::Inclusive
        };

        if chosen_mode != self.adaptive_current_mode {
            info!(
                "Adaptive cut switching from {:?} to {:?} (disparity={})",
                self.adaptive_current_mode, chosen_mode, disparity
            );
            self.adaptive_current_mode = chosen_mode;
        }

        match chosen_mode {
            CutStyle::Relaxed => self.compute_relaxed_cut(dag),
            CutStyle::Inclusive => self.compute_inclusive_cut(dag, last_committed_round),
            CutStyle::Adaptive => unreachable!(),
        }
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
        if let Some(by_author) = dag.get(&cut_round) {
            if let Some((_digest, cert)) = by_author.get(&leader_pk) {
                return Some(cert.clone());
            }
        }
        for r in (0..=cut_round).rev() {
            if let Some(by_author) = dag.get(&r) {
                if let Some((_digest, cert)) = by_author.get(&leader_pk) {
                    return Some(cert.clone());
                }
            }
        }
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

impl ConsensusProtocol for CutEngine {
    fn process_certificate(
        &mut self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> Result<(Outcome, Vec<CommittedSubDag>), ConsensusError> {
        debug!("CutEngine ({:?}) processing {:?}", self.style, certificate);
        let round = certificate.round();

        // 1) Insert the certificate into the DAG.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // 2) Compute the cut based on the selected strategy.
        let cut_round = match self.style {
            CutStyle::Relaxed => self.compute_relaxed_cut(&state.dag),
            CutStyle::Inclusive => {
                self.compute_inclusive_cut(&state.dag, state.last_committed_round)
            }
            CutStyle::Adaptive => {
                self.compute_adaptive_cut(&state.dag, state.last_committed_round)
            }
        };

        let cut_round = match cut_round {
            Some(r) => r,
            None => return Ok((Outcome::NotEnoughSupportForLeader, Vec::new())),
        };

        if cut_round <= state.last_committed_round {
            return Ok((Outcome::LeaderBelowCommitRound, Vec::new()));
        }

        // 3) Collect and commit.
        let sequence = Self::collect_prefix(&state.dag, cut_round);
        if sequence.is_empty() {
            return Ok((Outcome::LeaderNotFound, Vec::new()));
        }

        for c in &sequence {
            state.update(c, self.gc_depth);
        }

        let leader_cert = Self::pick_leader_for_subdag(&self.committee, &state.dag, cut_round)
            .unwrap_or_else(|| sequence.last().cloned().unwrap());

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
            "CutEngine commit at round {} (style={:?}, adaptive_mode={:?}) sub_dag_index={}",
            cut_round, self.style, self.adaptive_current_mode, next_sub_dag_index
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

    fn setup() -> (Committee, Vec<PublicKey>, BTreeSet<types::CertificateDigest>) {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let keys: Vec<_> = fixture.authorities().map(|a| a.public_key()).collect();
        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();
        (committee, keys, genesis)
    }

    #[tokio::test]
    async fn relaxed_cut_commit() {
        let (committee, keys, genesis) = setup();
        let (certs, _) =
            test_utils::make_optimal_certificates(&committee, 1..=12, &genesis, &keys);
        let store = make_consensus_store(&test_utils::temp_dir());
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        let mut state = ConsensusState::new(metrics);
        let mut engine = CutEngine::new(committee, store, 12, CutStyle::Relaxed);

        for c in certs {
            let _ = engine.process_certificate(&mut state, c);
        }
        assert!(state.last_committed_round > 0);
    }

    #[tokio::test]
    async fn inclusive_cut_commit() {
        let (committee, keys, genesis) = setup();
        let (certs, _) =
            test_utils::make_optimal_certificates(&committee, 1..=12, &genesis, &keys);
        let store = make_consensus_store(&test_utils::temp_dir());
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        let mut state = ConsensusState::new(metrics);
        let mut engine = CutEngine::new(committee, store, 12, CutStyle::Inclusive);

        for c in certs {
            let _ = engine.process_certificate(&mut state, c);
        }
        assert!(state.last_committed_round > 0);
    }

    #[tokio::test]
    async fn adaptive_cut_commit() {
        let (committee, keys, genesis) = setup();
        let (certs, _) =
            test_utils::make_optimal_certificates(&committee, 1..=12, &genesis, &keys);
        let store = make_consensus_store(&test_utils::temp_dir());
        let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        let mut state = ConsensusState::new(metrics);
        let mut engine = CutEngine::new(committee, store, 12, CutStyle::Adaptive);

        for c in certs {
            let _ = engine.process_certificate(&mut state, c);
        }
        assert!(state.last_committed_round > 0);
    }
}
