use crate::types::batch::ZkEVMBatch;

pub async fn fetch_batch_by_number(batch_number: u64) -> Option<ZkEVMBatch> {
    // TODO: Replace this with real DB or state access
    Some(ZkEVMBatch {
        acc_input_hash: "0xabc...".into(),
        blocks: vec!["0x123...".into(), "0x456...".into()],
        batch_l2_data: "0xdeadbeef...".into(),
        coinbase: "0x000000000000000000000000000000000000c0de".into(),
        global_exit_root: "0x...".into(),
        local_exit_root: "0x...".into(),
        state_root: "0x...".into(),
        closed: true,
        timestamp: format!("0x{:x}", 1720000000u64),
    })
}
