use reth::{
    network::NetworkHandle,
    primitives::{SealedBlock, U128},
};
use reth_eth_wire::NewBlock;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

pub struct BlockAnnouncer {}

impl BlockAnnouncer {
    pub fn spawn(
        mut block_event: Receiver<SealedBlock>,
        network_manager: NetworkHandle,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(block) = block_event.recv() => {
                        let block_no = block.number;
                        let hash = block.hash();
                        let new_block = NewBlock {
                            td: U128::from(block.difficulty),  // TODO: (panic) potential overflow by converting U256 to U128
                            block: block.into(),
                        };
                        network_manager.announce_block(new_block, hash);
                        tracing::info!("Announced {}-th block: {}", block_no, hash);
                    }
                }
            }
        })
    }
}
