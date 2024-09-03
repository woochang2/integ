use async_trait::async_trait;
use reth::{
    primitives::{BlockWithSenders, ChainSpec, Receipt},
    providers::ProviderError,
    revm::db::BundleState,
};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

use reth_interfaces::executor::BlockExecutionError;

use crate::{
    db::{SharableStateDBBox, ThreadSafeCacheState},
    types::ExecutableConsensusOutput,
    BlockchainProviderMDBX,
};

pub trait Executable {
    /// This takes a block and returns new [BlockWithSenders] since some execution algorithm reorders transactions.
    fn execute(
        &mut self,
        consensus_output: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), BlockExecutionError>;

    fn new_with_db(
        db: BlockchainProviderMDBX,
        cached_state: Option<ThreadSafeCacheState>,
        chain_spec: Arc<ChainSpec>,
    ) -> Self;

    fn take_bundle(&self) -> BundleState;

    fn state_helper<T, F: FnOnce(&SharableStateDBBox<ProviderError>) -> Result<T, ProviderError>>(
        &self,
        state_helper_function: F,
    ) -> Result<T, ProviderError>;
}

/// An abstraction for an executor in a sui PrimaryNode.
/// Executor receives ExecutableConsensusOutput from primary node calling [ExecutionState]::handle_consunsus_output, and it executes the block.
#[async_trait]
pub trait SuiExecutionAdapter {
    async fn run(
        &mut self,
        mut rx_executable_consensus_output: Receiver<ExecutableConsensusOutput>,
    );
}

/// An executor capable of executing a block in parallel.
pub(crate) trait ParallelBlockExecutor {
    /// The error type returned by the executor.
    type Error;

    /// Execute a block.
    fn execute(&mut self, block: BlockWithSenders) -> Result<(), Self::Error>;

    /// Executes the block and checks receipts.
    ///
    /// See [execute](BlockExecutor::execute) for more details.
    fn execute_and_verify_receipt(&mut self, block: BlockWithSenders) -> Result<(), Self::Error>;

    /// Runs the provided transactions and commits their state to the run-time database.
    ///
    /// The returned [BundleStateWithReceipts] can be used to persist the changes to disk, and
    /// contains the changes made by each transaction.
    ///
    /// The changes in [BundleStateWithReceipts] have a transition ID associated with them: there is
    /// one transition ID for each transaction (with the first executed tx having transition ID
    /// 0, and so on).
    ///
    /// The second returned value represents the total gas used by this block of transactions.
    ///
    /// See [execute](BlockExecutor::execute) for more details.
    fn execute_transactions(
        &mut self,
        block: BlockWithSenders,
    ) -> Result<(BlockWithSenders, Vec<Receipt>, u64), Self::Error>;

    // /// Return bundle state. This is output of executed blocks.
    // async fn take_output_state(&mut self) -> BundleStateWithReceipts;

    // /// Returns the size hint of current in-memory changes.
    // fn size_hint(&self) -> Option<usize>;
}
