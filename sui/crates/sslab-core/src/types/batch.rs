use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct ZkEVMBatch {
    pub acc_input_hash: String,
    pub blocks: Vec<String>,
    pub batch_l2_data: String,
    pub coinbase: String,
    pub global_exit_root: String,
    pub local_exit_root: String,
    pub state_root: String,
    pub closed: bool,
    pub timestamp: String,
}
