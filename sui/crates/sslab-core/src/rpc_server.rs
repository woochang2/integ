mod rpc;
use jsonrpsee::http_server::{HttpServerBuilder, RpcModule};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = HttpServerBuilder::default().build("127.0.0.1:8545").await?;
    let mut module = RpcModule::new(());

    rpc::register_batch_rpc_methods(&mut module);
    // optionally register other modules...

    server.start(module)?.await;
    Ok(())
}
