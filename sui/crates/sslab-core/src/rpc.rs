use crate::types::batch::ZkEVMBatch;
use jsonrpsee::RpcModule;

pub fn register_batch_rpc_methods(module: &mut RpcModule<()>) {
    module.register_async_method("zkevm_getBatchByNumber", |params, _| async move {
        let batch_number: u64 = params.one()?;  // extract param

        // TODO: Lookup batch from real storage
        let maybe_batch = crate::batch::fetch_batch_by_number(batch_number).await;

        match maybe_batch {
            Some(batch) => Ok(batch),
            None => Err(jsonrpsee::types::ErrorObject::owned(
                -32000,
                "Batch not found",
                Some(format!("Batch {} not available", batch_number)),
            )),
        }
    }).unwrap();
}
