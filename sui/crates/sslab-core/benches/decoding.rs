#![allow(dead_code)]
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use ethers_providers::{MockProvider, Provider};
use rayon::prelude::*;
use sslab_core::consensus_handler::decode_batch;
use sslab_execution::types::ExecutableEthereumBatch;
use sslab_execution::utils::test_utils::{SmallBankTransactionHandler, DEFAULT_CHAIN_ID};

const DEFAULT_BATCH_SIZE: usize = 250;
const DEFAULT_SKEWNESS: f32 = 0.0;

fn _get_smallbank_handler() -> SmallBankTransactionHandler {
    let provider = Provider::<MockProvider>::new(MockProvider::default());
    SmallBankTransactionHandler::new(provider, DEFAULT_CHAIN_ID)
}

fn _create_random_smallbank_workload_v2(
    skewness: f32,
    batch_size: usize,
    block_concurrency: usize,
) -> Vec<ExecutableEthereumBatch> {
    let handler = _get_smallbank_handler();

    handler.create_batches(batch_size, block_concurrency, skewness, 10_000)
}

fn json_decoding(c: &mut Criterion) {
    let param = 1..11;
    let mut group = c.benchmark_group("decoding ethereum transactions according to batchsize");
    for i in param {
        group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE * i) as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("# of batches", i),
            &i,
            |b, i| {
                b.iter_batched(
                    || {
                        let batches = _create_random_smallbank_workload_v2(
                            DEFAULT_SKEWNESS,
                            DEFAULT_BATCH_SIZE * i,
                            1,
                        );
                        batches
                            .into_par_iter()
                            .map(|batch| {
                                batch
                                    .data()
                                    .iter()
                                    .map(|tx| tx.envelope_encoded().0.to_vec())
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>()
                    },
                    |batches| {
                        for batch in batches {
                            let _ = decode_batch(batch);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

// TODO: bincode::deserialize is not working
// fn _bincode_encode(transaction: &EthereumTransaction) -> Vec<u8> {
//     bincode::serialize(&transaction.0).unwrap()
// }

// fn _bincode_decode(serialized_transaction: &Vec<u8>) -> TypedTransaction {
//     bincode::deserialize_from(serialized_transaction.clone().as_slice()).unwrap()
// }

// fn bincode_decoding(c: &mut Criterion) {
//     let param = 1..11;
//     let mut group = c.benchmark_group("decoding ethereum transactions according to # of batches");
//     for i in param {
//         group.throughput(Throughput::Elements((DEFAULT_BATCH_SIZE*i) as u64));
//         group.bench_with_input(
//             criterion::BenchmarkId::new("# of batches", i),
//             &i,
//             |b, i| {
//                 b.iter_batched(
//                     || {
//                         let batches = _create_random_smallbank_workload_v2(DEFAULT_SKEWNESS, DEFAULT_BATCH_SIZE, *i);
//                         batches
//                             .into_par_iter()
//                             .map(|batch|
//                                 batch.data().iter().map(|tx| _bincode_encode(tx)).collect::<Vec<_>>()
//                             )
//                             .collect::<Vec<_>>()
//                     },
//                     |batches| {
//                         for batch in batches {
//                             batch
//                                 .par_iter()
//                                 .map(|serialized_transaction| {
//                                     _bincode_decode(serialized_transaction)
//                                 })
//                                 .collect::<Vec<_>>();
//                         }
//                     },
//                     BatchSize::SmallInput
//                 );
//             }
//         );
//     }
// }

criterion_group!(benches, json_decoding);
criterion_main!(benches);
