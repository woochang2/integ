use async_trait::async_trait;
use fastcrypto::hash::Hash as _;
use futures::stream::FuturesUnordered;
use narwhal_executor::ExecutionState;
use narwhal_types::ConsensusOutput;
use rayon::prelude::*;
use sslab_execution::{
    traits::SuiExecutionAdapter,
    types::{ExecutableConsensusOutput, ExecutableEthereumBatch},
    TransactionSigned,
};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::{instrument, warn};

#[allow(dead_code)]
pub struct SimpleConsensusHandler {
    tx_executable_consensus_output: Sender<ExecutableConsensusOutput>,
    // tx_shutdown: Option<PreSubscribedBroadcastSender>,
    handles: FuturesUnordered<JoinHandle<()>>,
}

impl SimpleConsensusHandler {
    pub fn new<Executor>(mut executor: Executor) -> Self
    where
        Executor: SuiExecutionAdapter + Send + Sync + 'static,
    {
        let handles = FuturesUnordered::new();
        let (tx_executable_consensus_output, rx_executable_consensus_output) =
            tokio::sync::mpsc::channel(1000);

        handles.push(executor.run(rx_executable_consensus_output));

        Self {
            tx_executable_consensus_output,
            // tx_shutdown: Some(tx_shutdown),
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
                use trancing::info;
                // NOTE: This log entry is used to compute performance.
                consensus_output.sub_dag.certificates.iter().for_each(|cert| {
                    cert.header().payload().keys().for_each(|digest| info!("Consensus handler received a batch -> {:?}", digest));
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

async fn decode_batch(raw_batch: Vec<Vec<u8>>) -> Vec<TransactionSigned> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let batch = raw_batch
            .into_par_iter() //TODO: prioritized less than execution threads
            .map(|raw_tx| {
                TransactionSigned::decode_enveloped(&mut raw_tx.as_slice()).expect(
                    "No error occurs since every Tx has been validated in RPC server and workers",
                )
            })
            .collect::<Vec<_>>();

        let _ = send.send(batch).unwrap();
    });

    recv.await.unwrap()
}
