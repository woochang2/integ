use std::sync::Arc;

use async_trait::async_trait;
use executor::ExecutionState;
use fastcrypto::hash::Hash as _;
use itertools::Itertools;
use rayon::prelude::*;
use reth::network::NetworkHandle;
use sslab_execution::{
    db::ThreadSafeCacheState,
    executor::ParallelExecutor,
    traits::Executable,
    types::{ExecutableConsensusOutput, ExecutableEthereumBatch},
    ProviderFactoryMDBX, SslabChainSpec, TransactionSigned,
};
use sslab_p2p::block_announcer::BlockAnnouncer;
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{instrument, warn};
use types::{ConsensusOutput, PreSubscribedBroadcastSender};

#[allow(dead_code)]
pub struct SimpleConsensusHandler {
    tx_executable_consensus_output: Sender<ExecutableConsensusOutput>,
    tx_shutdown: PreSubscribedBroadcastSender,
    handles: Vec<JoinHandle<()>>,
}

impl SimpleConsensusHandler {
    pub fn new<ExecutionModel>(
        provider_factory: ProviderFactoryMDBX,
        chain_spec: Arc<SslabChainSpec>,
        preloaded_state: Option<ThreadSafeCacheState>,
        devp2p_network_manager: NetworkHandle,
    ) -> Self
    where
        ExecutionModel: Executable + Send + 'static,
    {
        let (tx_executable_consensus_output, rx_executable_consensus_output) =
            tokio::sync::mpsc::channel(1000);
        let mut tx_shutdown = PreSubscribedBroadcastSender::new(1);
        let (mut handles, subscribe_new_block) = ParallelExecutor::spawn::<ExecutionModel>(
            provider_factory,
            chain_spec,
            preloaded_state,
            rx_executable_consensus_output,
            tx_shutdown.subscribe(),
        );

        handles.push(BlockAnnouncer::spawn(
            subscribe_new_block,
            devp2p_network_manager,
        ));

        Self {
            tx_executable_consensus_output,
            tx_shutdown,
            handles,
        }
    }

    // pub async fn shutdown(&mut self) {
    //     // send the shutdown signal to the node
    //     let now = Instant::now();
    //     info!("Sending shutdown message to primary node");

    //     if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
    //         tx_shutdown
    //             .send()
    //             .expect("Couldn't send the shutdown signal to downstream components");
    //         self.tx_shutdown = None;
    //     }

    //     // Now wait until handles have been completed
    //     try_join_all(&mut self.handles).await.unwrap();

    //     info!(
    //         "Narwhal primary shutdown is complete - took {} seconds",
    //         now.elapsed().as_secs_f64()
    //     );
    // }
}

#[async_trait]
impl ExecutionState for SimpleConsensusHandler {
    /// This function will be called by Narwhal, after Narwhal sequenced this certificate.
    #[instrument(level = "trace", skip_all)]
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        let sub_dag_index = consensus_output.sub_dag.sub_dag_index;

        cfg_if::cfg_if! {
            if #[cfg(feature = "benchmark")] {
                use tracing::info;
                // NOTE: This log entry is used to compute performance.
                consensus_output.sub_dag.certificates.iter().for_each(|cert| {
                    cert.header.payload.keys().for_each(|digest| info!("Consensus handler received a batch -> {:?}", digest));
                });

                // NOTE: This log entry is used to compute performance.
                info!("Received consensus_output has {} batches at subdag_index {}.", consensus_output.sub_dag.num_batches(), sub_dag_index);
            }
        }

        /* (serialized, transaction, output_cert) */
        let mut ethereum_batches = vec![];

        for (cert, batches) in consensus_output.batches.into_iter() {
            assert_eq!(cert.header.payload.len(), batches.len());

            for batch in batches {
                assert!(cert.header.payload.contains_key(&batch.digest()));

                if batch.transactions.is_empty() {
                    continue;
                }

                let digest = batch.digest();

                let decoded_batch = decode_batch(batch.transactions).await;

                if !decoded_batch.is_empty() {
                    ethereum_batches.push(ExecutableEthereumBatch::new(decoded_batch, digest));
                } else {
                    warn!("Received an empty decoded batch at subdag_index {}. This couldn't possible.", sub_dag_index)
                }
            }
        }

        let executable_consensus_output = ExecutableConsensusOutput::new(ethereum_batches);

        if !executable_consensus_output.data().is_empty() {
            let _ = self
                .tx_executable_consensus_output
                .send(executable_consensus_output)
                .await;
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }
}

pub async fn decode_batch(raw_batch: Vec<Vec<u8>>) -> Vec<TransactionSigned> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let batch = raw_batch
            .into_par_iter() //TODO: prioritized less than execution threads
            .map(|raw_tx| {
                TransactionSigned::decode_enveloped(&mut raw_tx.as_slice()).expect(
                    "No error occurs since every Tx has been validated in RPC server and workers",
                )
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unique_by(|tx| tx.hash)
            .collect_vec();

        let _ = send.send(batch).unwrap();
    });

    recv.await.unwrap()
}
