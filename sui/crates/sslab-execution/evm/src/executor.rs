use core::panic;
use std::sync::Arc;

use narwhal_types::{BatchDigest, ConditionalBroadcastReceiver, PreSubscribedBroadcastSender};
use reth::{
    primitives::{
        proofs, Block, BlockHash, BlockNumber, BlockWithSenders, ChainSpec, Header, SealedBlock,
        SealedBlockWithSenders, SealedHeader, TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, U256,
    },
    providers::{
        BlockIdReader, BlockReader, BlockReaderIdExt, BlockSource, BundleStateWithReceipts,
        CanonChainTracker, Chain, ProviderError, StateProviderBox, StateProviderFactory,
    },
};

use reth_interfaces::{
    blockchain_tree::{BlockchainTreeEngine, BlockchainTreeViewer},
    consensus::ForkchoiceState,
    executor::BlockExecutionError,
    RethError, RethResult,
};

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{info, trace};

use crate::{
    db::ThreadSafeCacheState,
    evm_processor::EVMProcessor,
    revm_utiles::{recover_senders, unpack_batches},
    traits::{Executable, ParallelBlockExecutor as _},
    types::ExecutableConsensusOutput,
    BlockchainProviderMDBX,
};

/// [ParallelExecutor] spawns the two components: [Inner] and [PostProcessor].
/// [Inner] is responsible for recovering senders, executing the transactions, create a new header, and sealing the block.
/// [PostProcessor] is responsible for persisting the block.
/// The two components are performed in a pipelined fashion.
/// Accordingly, [Inner] can execute the next block when the parent block is being persisted.
/// [Inner] must wait for the parent block to be persisted before create a new header because the state root is calculated based on the parent block.
pub struct ParallelExecutor {
    _handles: Vec<JoinHandle<()>>,
}

impl ParallelExecutor {
    pub fn spawn<ParallelExecutionModel>(
        blockchain_provider: BlockchainProviderMDBX,
        chain_spec: Arc<ChainSpec>,
        preloaded_state: Option<ThreadSafeCacheState>,
        rx_executable_consensus_output: Receiver<ExecutableConsensusOutput>,
        rx_shutdown: ConditionalBroadcastReceiver,
    ) -> (
        Vec<JoinHandle<()>>,
        tokio::sync::mpsc::Receiver<SealedBlock>,
    )
    where
        ParallelExecutionModel: Executable + Send + 'static,
    {
        let latest_header = blockchain_provider
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        let (tx_execution_output, rx_execution_output) = tokio::sync::mpsc::channel(1);
        let (tx_latest_block_hash, rx_latest_block_hash) = tokio::sync::mpsc::channel(100);
        let (notify_new_block, subscribe_new_block) = tokio::sync::mpsc::channel(100);

        let mut tx_shutdown_post_processor = PreSubscribedBroadcastSender::new(1);

        let post_processor = PostProcessor::spawn(
            rx_execution_output,
            blockchain_provider.clone(),
            tx_latest_block_hash,
            // metrics.clone(),
            tx_shutdown_post_processor.subscribe(),
        );

        let inner = Inner::<ParallelExecutionModel>::spawn(
            blockchain_provider,
            chain_spec.clone(),
            preloaded_state,
            latest_header,
            rx_executable_consensus_output,
            tx_execution_output,
            rx_latest_block_hash,
            // metrics,
            rx_shutdown,
            tx_shutdown_post_processor,
            notify_new_block,
        );

        (Vec::from([inner, post_processor]), subscribe_new_block)
    }
}

pub struct Inner<ParallelExecutionModel> {
    /// The latest block header processed by the executor.
    latest: Header,

    /// The hash of the latest block header processed by the executor.
    latest_hash: BlockHash,

    chain_spec: Arc<ChainSpec>,

    stateroot_provider: StateProviderBox,

    executor: EVMProcessor<ParallelExecutionModel>,

    /// The channel to send the sealed block to the post processor for persist.
    tx_execution_output: Sender<(Chain, Vec<BatchDigest>)>,

    /// The channel to receive the block number of the parent block finished persisting.
    wait_post_processing: Receiver<BlockNumber>,

    /// The block number sent to the post processor for persisting.
    post_processing_request: Option<BlockHash>,

    /// The channel to broadcast the new block to the devp2p network
    /// so that other full nodes can sycn with the consensus network.
    /// See [sslab_p2p::BlockAnnouncer] for more details.
    broadcast_new_block: tokio::sync::mpsc::Sender<SealedBlock>,
}

impl<ParallelExecutionModel: Executable + Send + 'static> Inner<ParallelExecutionModel> {
    pub fn spawn(
        blockchain_provider: BlockchainProviderMDBX,
        chain_spec: Arc<ChainSpec>,
        preloaded_state: Option<ThreadSafeCacheState>,
        latest_header: SealedHeader,
        rx_executable_consensus_output: Receiver<ExecutableConsensusOutput>,
        tx_execution_output: Sender<(Chain, Vec<BatchDigest>)>,
        wait_post_processing: Receiver<BlockNumber>,
        // metrics: ExecutionMetrics,
        rx_shutdown: ConditionalBroadcastReceiver,
        tx_shutdown_post_processor: PreSubscribedBroadcastSender,
        broadcast_new_block: tokio::sync::mpsc::Sender<SealedBlock>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let (latest, latest_hash) = latest_header.split();

            let executor = EVMProcessor::<ParallelExecutionModel>::new(
                blockchain_provider.clone(),
                chain_spec.clone(),
                preloaded_state,
            );

            Self {
                latest,
                latest_hash,
                chain_spec,
                stateroot_provider: blockchain_provider.latest().unwrap(),
                executor,
                tx_execution_output,
                wait_post_processing,
                post_processing_request: None,
                broadcast_new_block,
                // metrics,
            }
            .run(
                tx_shutdown_post_processor,
                rx_shutdown,
                rx_executable_consensus_output,
            )
            .await;
        })
    }

    async fn run(
        &mut self,
        tx_shutdown_post_processor: PreSubscribedBroadcastSender,
        mut rx_shutdown: ConditionalBroadcastReceiver,
        mut rx_executable_consensus_output: Receiver<ExecutableConsensusOutput>,
    ) {
        loop {
            tokio::select! {
                Some(consensus_output) = rx_executable_consensus_output.recv() => {
                    cfg_if::cfg_if! {
                        if #[cfg(feature = "benchmark")] {
                            use tracing::info;
                            // NOTE: This log entry is used to compute performance.
                            consensus_output.data().iter().for_each(|batch_digest|
                                info!("Received Batch -> {:?}", batch_digest.digest())
                            );
                        }
                    }

                    let (_digests, transactions) = unpack_batches(consensus_output.take_data()).await;
                    // let latency = tokio::time::Instant::now();
                    match self.execute_and_persist(transactions, _digests.clone()).await {
                        Ok(()) => {
                            cfg_if::cfg_if! {
                                if #[cfg(feature = "benchmark")] {
                                    // NOTE: This log entry is used to compute performance.
                                    _digests.iter().for_each(|batch_digest|
                                        tracing::info!("Executed Batch -> {:?}", batch_digest)
                                    );
                                }
                            }
                        },
                        Err(e) => tracing::error!("Error occures during execution: {e:?}")
                    }
                    // self.metrics
                    //     .record(latency.elapsed().as_micros(), LatencyType::Total);
                }

                Ok(()) = rx_shutdown.receiver.recv() => {
                    let _ = tx_shutdown_post_processor.send();
                    println!("Inner shutting down");
                    return;
                }
            }
        }
    }

    /// Inserts a new header+body pair
    pub(crate) fn record_new_block(&mut self, header: &SealedHeader) {
        self.latest = header.header().clone();
        self.latest_hash = header.hash();
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(&self) -> Header {
        //* Hack: the actual timestamp is not appropriate for OX-like architecture
        let timestamp = std::time::Duration::from_secs(self.latest.number + 1).as_secs();
        // let timestamp = SystemTime::now()
        //     .duration_since(UNIX_EPOCH)
        //     .unwrap_or_default()
        //     .as_secs();

        // check previous block for base fee
        let base_fee_per_gas = self
            .latest
            .next_block_base_fee(self.chain_spec.base_fee_params(timestamp));

        Header {
            parent_hash: self.latest_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: Default::default(),
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::from(2),
            number: self.latest.number + 1,
            gas_limit: self.chain_spec.genesis().gas_limit,
            gas_used: 0,
            timestamp,
            mix_hash: Default::default(),
            nonce: 0,
            base_fee_per_gas,
            blob_gas_used: None,
            excess_blob_gas: None,
            extra_data: Default::default(),
            parent_beacon_block_root: None,
        }
    }

    /// Executes the block with the given block and senders, on the provided [EVMProcessor].
    ///
    /// This returns the poststate from execution and post-block changes, as well as the gas used.
    pub(crate) async fn execute_inner(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, BundleStateWithReceipts, u64), BlockExecutionError> {
        trace!(target: "ParallelExecutor::Inner", transactions=?&block.body, "executing transactions");
        // TODO: there isn't really a parent beacon block root here, so not sure whether or not to
        // call the 4788 beacon contract
        // let mut executor = self.executor.lock();

        self.executor.set_first_block(block.number);

        let (mut new_block, receipts, gas_used) = self.executor.execute_transactions(block)?;

        if !new_block.body.is_empty() {
            new_block.block.header.transactions_root =
                proofs::calculate_transaction_root(new_block.body.as_slice());
        }

        // add post execution state change
        // Withdrawals, rewards etc.
        //* No mining reward or withdrawals in PoA
        // self.executor
        //     .apply_post_execution_state_change(&new_block.block)?;

        // apply post block changes
        Ok((
            new_block,
            self.executor.take_output_state(receipts),
            gas_used,
        ))
    }

    /// Fills in the post-execution header fields based on the given BundleState and gas used.
    /// In doing this, the state root is calculated and the final header is returned.
    pub(crate) fn complete_header(
        &self,
        mut header: Header,
        transactions: &[TransactionSigned],
        bundle_state: &BundleStateWithReceipts,
        gas_used: u64,
    ) -> Result<Header, BlockExecutionError> {
        use reth::primitives::{
            constants::{EMPTY_RECEIPTS, EMPTY_TRANSACTIONS},
            Bloom, ReceiptWithBloom,
        };

        header.transactions_root = if transactions.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            proofs::calculate_transaction_root(transactions)
        };

        let receipts = bundle_state.receipts_by_block(header.number);
        header.receipts_root = if receipts.is_empty() {
            EMPTY_RECEIPTS
        } else {
            let receipts_with_bloom = receipts
                .iter()
                .map(|r| (*r).clone().expect("receipts have not been pruned").into())
                .collect::<Vec<ReceiptWithBloom>>();
            header.logs_bloom = receipts_with_bloom
                .iter()
                .fold(Bloom::ZERO, |bloom, r| bloom | r.bloom);
            proofs::calculate_receipt_root(&receipts_with_bloom)
        };

        header.gas_used = gas_used;

        // calculate the state root
        let state_root = self.stateroot_provider.state_root(bundle_state).unwrap();
        header.state_root = state_root;
        Ok(header)
    }

    /// Builds and executes a new block with the given transactions, on the provided [EVMProcessor].
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub async fn execute_and_persist(
        &mut self,
        transactions: Vec<TransactionSigned>,
        _digests: Vec<BatchDigest>,
    ) -> RethResult<()> {
        let header = self.build_header_template();
        let block = recover_senders(transactions, header).await?;

        trace!(target: "ParallelExecutor::Inner", transactions=?&block.body, "executing transactions");
        let (new_block, bundle_state, gas_used) = self.execute_inner(block).await?;
        let BlockWithSenders { block, senders } = new_block;
        let Block { header, body, .. } = block;

        trace!(target: "ParallelExecutor::Inner", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // wait for the parent block to be persisted
        if let Some(parent_hash) = self.post_processing_request {
            loop {
                let block_no = self.wait_post_processing.recv().await.unwrap();
                if block_no == header.parent_num_hash().number
                    && parent_hash == header.parent_num_hash().hash
                {
                    break;
                } else {
                    panic!(
                        "Received block number {} while waiting for block number {}",
                        block_no,
                        header.parent_num_hash().number
                    );
                }
            }
        }

        let new_header = self.complete_header(header, body.as_slice(), &bundle_state, gas_used)?;

        trace!(target: "ParallelExecutor::Inner", root=?new_header.state_root, ?body, "calculated root");

        let sealed_block = Block {
            header: new_header,
            body,
            ommers: vec![],
            withdrawals: None,
        }
        .seal_slow();

        // TODO: can we avoid cloning here?
        let _ = self.broadcast_new_block.send(sealed_block.clone()).await;

        let sealed_block_with_senders = SealedBlockWithSenders {
            block: sealed_block,
            senders,
        };

        self.record_new_block(&sealed_block_with_senders.header);

        let chain = Chain::new(vec![sealed_block_with_senders], bundle_state, None);

        // send the sealed block to the post processor
        self.post_processing_request = Some(self.latest_hash);
        let _ = self
            .tx_execution_output
            .send((chain, _digests))
            .await
            .unwrap();

        Ok(())
    }
}

pub struct PostProcessor {
    tx_latest_blocknum: Sender<BlockNumber>,
    blockchain: BlockchainProviderMDBX,
    // metrics: ExecutionMetrics,
}

impl PostProcessor {
    pub fn spawn(
        rx_execution_output: Receiver<(Chain, Vec<BatchDigest>)>,
        blockchain: BlockchainProviderMDBX,
        tx_latest_blocknum: Sender<BlockNumber>,
        // metrics: ExecutionMetrics,
        rx_shutdown: ConditionalBroadcastReceiver,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                blockchain,
                tx_latest_blocknum,
                // metrics,
            }
            .run(rx_shutdown, rx_execution_output)
            .await;
        })
    }

    async fn run(
        &self,
        mut rx_shutdown: ConditionalBroadcastReceiver,
        mut rx_execution_output: Receiver<(Chain, Vec<BatchDigest>)>,
    ) {
        loop {
            tokio::select! {
                Some((chain, _digests)) = rx_execution_output.recv() => {
                    let block_hash = chain.tip().hash();
                    let block_number = chain.tip().number;

                    let _ = self.blockchain.tree.insert_chain(chain);

                    let state = ForkchoiceState {
                        head_block_hash: block_hash,
                        finalized_block_hash: block_hash,
                        safe_block_hash: block_hash,
                    };

                    // let now = tokio::time::Instant::now();
                    match self.blockchain.make_canonical(&block_hash) {
                        Ok(reth_interfaces::blockchain_tree::CanonicalOutcome::Committed { head }) => {
                            trace!(target: "ParallelExecutor::PostProcesscor", ?head, "block committed")
                        }
                        Ok(reth_interfaces::blockchain_tree::CanonicalOutcome::AlreadyCanonical {
                            header,
                        }) => {
                            panic!("Block already canonical: {:?}", header);
                        }
                        Err(e) => {
                            panic!("Error making block canonical: {:?}", e);
                        }
                    }

                    match self.ensure_consistent_state(state).unwrap() {
                        Some(false) => {
                            panic!("Forkchoice state is inconsistent after block execution");
                        }
                        _ => {}
                    };
                    // self.metrics
                    //     .record(now.elapsed().as_micros(), LatencyType::Persistence);

                    let _ = self.tx_latest_blocknum.send(block_number).await;

                    cfg_if::cfg_if! {
                        if #[cfg(feature = "benchmark")] {
                            // NOTE: This log entry is used to compute performance.
                            _digests.iter().for_each(|batch_digest|
                                tracing::info!("Persisted Batch -> {:?}", batch_digest)
                            );
                        }
                    }
                }

                _ = rx_shutdown.receiver.recv() => {
                    println!("PostProcessor shutting down");
                    return;
                }
            }
        }
    }

    /// Ensures that the given forkchoice state is consistent, assuming the head block has been
    /// made canonical. This takes a status as input, and will only perform consistency checks if
    /// the input status is VALID.
    ///
    /// If the forkchoice state is consistent, this will return Ok(None). Otherwise, this will
    /// return an instance of [OnForkChoiceUpdated] that is INVALID.
    ///
    /// This also updates the safe and finalized blocks in the [CanonChainTracker], if they are
    /// consistent with the head block.
    fn ensure_consistent_state(&self, state: ForkchoiceState) -> RethResult<Option<bool>> {
        // Ensure that the finalized block, if not zero, is known and in the canonical chain
        // after the head block is canonicalized.
        //
        // This ensures that the finalized block is consistent with the head block, i.e. the
        // finalized block is an ancestor of the head block.
        if !state.finalized_block_hash.is_zero()
            && !self.blockchain.is_canonical(state.finalized_block_hash)?
        {
            return Ok(Some(false));
        }

        // Finalized block is consistent, so update it in the canon chain tracker.
        self.update_finalized_block(state.finalized_block_hash)?;

        // Also ensure that the safe block, if not zero, is known and in the canonical chain
        // after the head block is canonicalized.
        //
        // This ensures that the safe block is consistent with the head block, i.e. the safe
        // block is an ancestor of the head block.
        if !state.safe_block_hash.is_zero()
            && !self.blockchain.is_canonical(state.safe_block_hash)?
        {
            return Ok(Some(false));
        }

        // Safe block is consistent, so update it in the canon chain tracker.
        self.update_safe_block(state.safe_block_hash)?;

        Ok(None)
    }

    /// Updates the tracked finalized block if we have it
    ///
    /// Returns an error if the block is not found.
    #[inline]
    fn update_finalized_block(&self, finalized_block_hash: B256) -> RethResult<()> {
        if !finalized_block_hash.is_zero() {
            if self.blockchain.finalized_block_hash()? == Some(finalized_block_hash) {
                // nothing to update
                return Ok(());
            }

            let finalized = self
                .blockchain
                .find_block_by_hash(finalized_block_hash, BlockSource::Any)?
                .ok_or_else(|| {
                    RethError::Provider(ProviderError::UnknownBlockHash(finalized_block_hash))
                })?;
            self.blockchain.finalize_block(finalized.number);
            self.blockchain
                .set_finalized(finalized.header.seal(finalized_block_hash));
        }
        Ok(())
    }

    /// Updates the tracked safe block if we have it
    ///
    /// Returns an error if the block is not found.
    #[inline]
    fn update_safe_block(&self, safe_block_hash: B256) -> RethResult<()> {
        if !safe_block_hash.is_zero() {
            if self.blockchain.safe_block_hash()? == Some(safe_block_hash) {
                // nothing to update
                return Ok(());
            }

            let safe = self
                .blockchain
                .find_block_by_hash(safe_block_hash, BlockSource::Any)?
                .ok_or_else(|| {
                    RethError::Provider(ProviderError::UnknownBlockHash(safe_block_hash))
                })?;
            self.blockchain.set_safe(safe.header.seal(safe_block_hash));
        }
        Ok(())
    }
}
