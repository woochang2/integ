use std::sync::Arc;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use parking_lot::RwLock;
use reth::primitives::ChainSpec;
use sslab_execution::executor::Inner;
use sslab_execution::traits::Executable;
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::smallbank_contract_benchmark::cache_state_with_smallbank_contract;
use sslab_execution::utils::test_utils::default_chain_spec;
use sslab_execution::utils::{
    smallbank_contract_benchmark::get_smallbank_handler, test_utils::convert_into_block,
};
use sslab_execution::{get_provider_factory, get_provider_factory_rw, ProviderFactoryMDBX};
use sslab_execution_serial::SerialExecutor;

const DEFAULT_BATCH_SIZE: usize = 200;

// fn _get_serial_executor() -> SerialExecutor<InMemoryConcurrentDB> {
//     let memory_storage = concurrent_memory_database();
//     let chain_spec = Arc::new(default_chain_spec());
//     SerialExecutor::new(memory_storage, chain_spec)
// }

fn _get_serial_executor(provider_factory: ProviderFactoryMDBX) -> SerialExecutor {
    use reth::providers::ChainSpecProvider;
    let chain_spec = provider_factory.chain_spec();
    SerialExecutor::new_with_db(
        provider_factory,
        Some(cache_state_with_smallbank_contract()),
        chain_spec,
    )
}

fn _get_serial_executor_with_evm_processor(
    provider_factory: ProviderFactoryMDBX,
    chain_spec: Arc<ChainSpec>,
) -> Inner<SerialExecutor> {
    Inner::<SerialExecutor>::new(
        provider_factory,
        chain_spec,
        Some(cache_state_with_smallbank_contract()),
    )
}

fn _create_random_smallbank_workload(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch> {
    let handler = get_smallbank_handler();
    handler.create_batches(batch_size, block_concurrency, skewness, 100_000)
}

fn serial(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 1..81;
    let mut group = c.benchmark_group("Serial");

    let chain_spec = Arc::new(default_chain_spec());
    let provider_factory = get_provider_factory(chain_spec.clone());

    for zipfian in s {
        for i in param.clone() {
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "blocksize",
                    format!("(zipfian: {zipfian}, #batch: {i})"),
                ),
                &i,
                |b, i| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output =
                                    convert_into_block(_create_random_smallbank_workload(
                                        zipfian,
                                        DEFAULT_BATCH_SIZE,
                                        *i,
                                    ));
                                let serial = _get_serial_executor(provider_factory.clone());
                                (serial, consensus_output)
                            },
                            |(mut serial, consensus_output)| async move {
                                let _ = serial.execute(consensus_output);
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
        }
    }
}

fn serial_with_storage_op(c: &mut Criterion) {
    let s = [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    let param = 80..81;
    let mut group = c.benchmark_group("Serial with storage operations");

    let chain_spec = Arc::new(default_chain_spec());
    let factory = get_provider_factory_rw(chain_spec.clone());

    for zipfian in s {
        for i in param.clone() {
            let latency_metrics = Arc::new(RwLock::new(vec![]));
            group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "latency",
                    format!("(zipfian: {zipfian}, #batch: {i})"),
                ),
                &(i, latency_metrics.clone()),
                |b, (i, metrics)| {
                    b.to_async(tokio::runtime::Runtime::new().unwrap())
                        .iter_batched(
                            || {
                                let consensus_output = _create_random_smallbank_workload(
                                    zipfian,
                                    DEFAULT_BATCH_SIZE * i,
                                    1,
                                )
                                .into_iter()
                                .flat_map(|batch| batch.take_data())
                                .collect();

                                let serial = _get_serial_executor_with_evm_processor(
                                    factory.clone(),
                                    chain_spec.clone(),
                                );
                                (serial, consensus_output)
                            },
                            |(mut serial, consensus_output)| async move {
                                let now = std::time::Instant::now();
                                match serial.execute_and_persist(consensus_output).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("Error: {:?}", e);
                                    }
                                };
                                let elapsed = now.elapsed().as_micros();
                                metrics.write().push((elapsed, serial.metrics.report()));
                            },
                            BatchSize::SmallInput,
                        );
                },
            );
            let len = latency_metrics.read().len() as f64;
            if len == 0.0 {
                continue;
            }

            let (
                mut sender_recovery,
                mut header_creation,
                mut block_sealing,
                mut execution_latency,
                mut persistence_latency,
                mut total,
            ) = (0f64, 0f64, 0f64, 0f64, 0f64, 0f64);

            for (a1, a2) in latency_metrics.read().iter() {
                total += *a1 as f64;
                sender_recovery += a2.0 as f64;
                header_creation += a2.1 as f64;
                block_sealing += a2.2 as f64;
                execution_latency += a2.3 as f64;
                persistence_latency += a2.4 as f64;
            }
            total /= len;
            sender_recovery /= len;
            header_creation /= len;
            block_sealing /= len;
            execution_latency /= len;
            persistence_latency /= len;
            println!(
                "Total: {:.4}, sender_recovery: {:.4}, header_creation: {:.4}, block_sealing: {:.4}, execution: {:.4}, persistence: {:.4}",
                total/1000.0, sender_recovery/1000.0, header_creation/1000.0, block_sealing/1000.0, execution_latency/1000.0, persistence_latency/1000.0
            );
            println!(
                "Ktps: {:.4}",
                (DEFAULT_BATCH_SIZE * i) as f64 / (total / 1000.0)
            );
        }
    }
}

criterion_group!(benches, serial_with_storage_op, serial);
criterion_main!(benches);
