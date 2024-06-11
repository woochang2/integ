use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use incr_stats::incr::Stats;
use parking_lot::RwLock;
use reth::{
    primitives::{
        constants::ETHEREUM_BLOCK_GAS_LIMIT, proofs, Block, BlockWithSenders, ChainSpec, Header,
        SealedBlockWithSenders, SealedHeader, TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, U256,
    },
    providers::{
        BlockIdReader, BlockReader, BlockReaderIdExt, BlockSource, BundleStateWithReceipts,
        CanonChainTracker, Chain, ProviderError,
    },
    revm::db::states::bundle_state::BundleRetention,
};

use reth_interfaces::{
    blockchain_tree::{BlockchainTreeEngine, BlockchainTreeViewer},
    consensus::ForkchoiceState,
    executor::BlockExecutionError,
    RethError, RethResult,
};

use tokio::sync::mpsc::Receiver;
use tracing::{info, trace};

use crate::{
    blockchain_provider,
    db::ThreadSafeCacheState,
    evm_processor::EVMProcessor,
    get_provider_factory_rw,
    revm_utiles::{recover_senders, unpack_batches},
    traits::{Executable, ParallelBlockExecutor as _, SuiExecutionAdapter},
    types::ExecutableConsensusOutput,
    BlockchainProviderMDBX, ProviderFactoryMDBX,
};

/// Client is [reth::provider::BlockchainProvider].
pub struct ParallelExecutor<ParallelExecutionModel> {
    // rx_shutdown: ConditionalBroadcastReceiver,
    inner: Inner<ParallelExecutionModel>,
}

#[async_trait]
impl<ParallelExecutionModel: Executable + Send + 'static> SuiExecutionAdapter
    for ParallelExecutor<ParallelExecutionModel>
{
    async fn run(
        &mut self,
        mut rx_executable_consensus_output: Receiver<ExecutableConsensusOutput>,
    ) {
        while let Some(consensus_output) = rx_executable_consensus_output.recv().await {
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
            match self.inner.execute_and_persist(transactions).await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("Error executing block: {:?}", e);
                }
            }

            cfg_if::cfg_if! {
                if #[cfg(feature = "benchmark")] {
                    // NOTE: This log entry is used to compute performance.
                    _digests.iter().for_each(|batch_digest|
                        info!("Executed Batch -> {:?}", batch_digest)
                    );
                }
            }
        }
    }
}

impl<ParallelExecutionModel: Executable + Send + 'static> ParallelExecutor<ParallelExecutionModel> {
    pub fn new(
        // rx_shutdown: ConditionalBroadcastReceiver,
        chain_spec: Arc<ChainSpec>,
        preloaded_state: Option<ThreadSafeCacheState>,
    ) -> Self {
        Self {
            // rx_shutdown,
            inner: Inner::new(
                get_provider_factory_rw(chain_spec.clone()),
                chain_spec,
                preloaded_state,
            ),
        }
    }
}

pub struct Inner<ParallelExecutionModel> {
    pub(crate) latest: Arc<RwLock<Header>>,
    pub(crate) latest_hash: Arc<RwLock<B256>>,

    pub(crate) chain_spec: Arc<ChainSpec>,

    pub(crate) db: ProviderFactoryMDBX,

    // pub(crate) provider_factory: ProviderFactoryMDBX,
    pub(crate) executor: EVMProcessor<'static, ParallelExecutionModel>,

    pub(crate) blockchain: BlockchainProviderMDBX,

    pub metrics: ExecutionMetrics,
}

impl<ParallelExecutionModel: Executable + 'static> Inner<ParallelExecutionModel> {
    pub fn new(
        factory: ProviderFactoryMDBX,
        chain_spec: Arc<ChainSpec>,
        preloaded_state: Option<ThreadSafeCacheState>,
    ) -> Self {
        let blockchain_provider = blockchain_provider(factory.clone());

        let best_header = blockchain_provider
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        let (header, best_hash) = best_header.split();

        let executor = EVMProcessor::<ParallelExecutionModel>::new(
            factory.clone(),
            chain_spec.clone(),
            preloaded_state,
        );

        Self {
            latest: Arc::new(RwLock::new(header)),
            latest_hash: Arc::new(RwLock::new(best_hash)),
            chain_spec,
            db: factory,
            executor,
            blockchain: blockchain_provider,
            metrics: ExecutionMetrics::default(),
        }
    }

    /// Inserts a new header+body pair
    pub(crate) fn record_new_block(&self, header: &SealedHeader) {
        *self.latest.write() = header.header().clone();
        *self.latest_hash.write() = header.hash();
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        // transactions: &[TransactionSigned],
        chain_spec: Arc<ChainSpec>,
    ) -> Header {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // check previous block for base fee
        let base_fee_per_gas = self
            .latest
            .read()
            .next_block_base_fee(chain_spec.base_fee_params(timestamp));

        Header {
            parent_hash: *self.latest_hash.read(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: Default::default(),
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::from(2),
            number: self.latest.read().number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
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

        // header.transactions_root = if transactions.is_empty() {
        //     EMPTY_TRANSACTIONS
        // } else {
        //     proofs::calculate_transaction_root(transactions)
        // };

        // header
    }

    /// Executes the block with the given block and senders, on the provided [EVMProcessor].
    ///
    /// This returns the poststate from execution and post-block changes, as well as the gas used.
    pub(crate) async fn execute(
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

        // Save receipts.
        self.executor.save_receipts(receipts)?;

        // add post execution state change
        // Withdrawals, rewards etc.
        //* No mining reward or withdrawals in OX architecture!
        // self.executor.apply_post_execution_state_change(block)?;

        // merge transitions
        self.executor
            .state
            .merge_transitions(BundleRetention::Reverts);

        // apply post block changes
        Ok((new_block, self.executor.take_output_state(), gas_used))
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
        let state_root = self
            .db
            .latest()
            .map_err(|_| BlockExecutionError::ProviderError)?
            .state_root(bundle_state)
            .unwrap();
        header.state_root = state_root;
        Ok(header)
    }

    /// Builds and executes a new block with the given transactions, on the provided [EVMProcessor].
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub async fn execute_and_persist(
        &mut self,
        transactions: Vec<TransactionSigned>,
    ) -> RethResult<()> {
        let now = tokio::time::Instant::now();
        let header = self.build_header_template(self.chain_spec.clone());
        let mut header_creation_latency = now.elapsed().as_micros();

        let now = tokio::time::Instant::now();
        let block = recover_senders(transactions, header).await?;
        self.metrics
            .record(now.elapsed().as_micros(), LatencyType::SenderRecovery);

        tracing::debug!(target: "ParallelExecutor::Inner", block_number=?block.number, transactions=?block.body.len(), "executing transactions");

        // now execute the block
        let now = tokio::time::Instant::now();
        let (new_block, bundle_state, gas_used) = self.execute(block).await?;
        self.metrics
            .record(now.elapsed().as_micros(), LatencyType::BlockExecution);

        let BlockWithSenders { block, senders } = new_block;
        let Block { header, body, .. } = block;

        tracing::debug!(target: "ParallelExecutor::Inner", block_number=?header.number, "executed block, calculating state root and completing header");

        // fill in the rest of the fields
        let now = tokio::time::Instant::now();
        let new_header = self.complete_header(header, body.as_slice(), &bundle_state, gas_used)?;
        header_creation_latency += now.elapsed().as_micros();
        self.metrics
            .record(header_creation_latency, LatencyType::HeaderCreation);

        tracing::debug!(target: "ParallelExecutor::Inner", block_number=?new_header.number, root=?new_header.state_root, "calculated root");

        // seal the block
        let sealed_block = SealedBlockWithSenders {
            block: Block {
                header: new_header.clone(),
                body,
                ommers: vec![],
                withdrawals: None,
            }
            .seal_slow(),
            senders,
        };

        let chain = Chain::new(vec![sealed_block.clone()], bundle_state.clone(), None);
        let _ = self.blockchain.tree.insert_chain(chain);

        let state = ForkchoiceState {
            head_block_hash: sealed_block.hash(),
            finalized_block_hash: sealed_block.hash(),
            safe_block_hash: sealed_block.hash(),
        };

        let now = tokio::time::Instant::now();
        match self.blockchain.make_canonical(&sealed_block.hash()) {
            Ok(reth_interfaces::blockchain_tree::CanonicalOutcome::Committed { head }) => {
                tracing::debug!(target: "ParallelExecutor::Inner", block_number=?head.number, header=?head.hash(), "block committed");
                self.record_new_block(&head);
            }
            Ok(reth_interfaces::blockchain_tree::CanonicalOutcome::AlreadyCanonical { header }) => {
                panic!("Block already canonical: {:?}", header);
            }
            Err(e) => {
                panic!("Error making block canonical: {:?}", e);
            }
        }

        match self.ensure_consistent_state(state)? {
            Some(false) => {
                panic!("Forkchoice state is inconsistent after block execution");
            }
            _ => {}
        };
        self.metrics
            .record(now.elapsed().as_micros(), LatencyType::Persistence);

        Ok(())
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
    fn ensure_consistent_state(&mut self, state: ForkchoiceState) -> RethResult<Option<bool>> {
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

#[derive(Default)]
pub struct ExecutionMetrics {
    sender_recovery_latency: Stats,
    header_creation_latency: Stats,
    block_execution_latency: Stats,
    persistence_latency: Stats,
}

pub enum LatencyType {
    HeaderCreation,
    BlockExecution,
    Persistence,
    SenderRecovery,
}

impl ExecutionMetrics {
    pub fn report(&self) -> (f64, f64, f64, f64) {
        (
            self.sender_recovery_latency.mean().unwrap_or_default(),
            self.header_creation_latency.mean().unwrap_or_default(),
            self.block_execution_latency.mean().unwrap_or_default(),
            self.persistence_latency.mean().unwrap_or_default(),
        )
    }

    fn record(&mut self, latency: u128, latency_type: LatencyType) {
        match latency_type {
            LatencyType::HeaderCreation => {
                self.header_creation_latency.update(latency as f64).unwrap();
            }
            LatencyType::BlockExecution => {
                self.block_execution_latency.update(latency as f64).unwrap();
            }
            LatencyType::Persistence => {
                self.persistence_latency.update(latency as f64).unwrap();
            }
            LatencyType::SenderRecovery => {
                self.sender_recovery_latency.update(latency as f64).unwrap();
            }
        }
    }
}
