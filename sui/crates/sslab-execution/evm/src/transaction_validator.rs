use async_trait::async_trait;
use narwhal_types::Batch;
use narwhal_worker::TransactionValidator;
use rayon::prelude::*;
use reth::primitives::{alloy_primitives::private::alloy_rlp::Decodable, TransactionSigned};

#[derive(Clone, Debug, Default)]
pub struct EthereumTxValidator;

#[async_trait]
impl TransactionValidator for EthereumTxValidator {
    type Error = eyre::Report;

    /// Determines if a transaction valid for the worker to consider putting in a batch
    fn validate(&self, t: &[u8]) -> Result<(), Self::Error> {
        let mut raw_tx = t;
        TransactionSigned::decode(&mut raw_tx)?;
        Ok(())
    }

    /// Determines if this batch can be voted on
    fn validate_batch(&self, b: &Batch) -> Result<(), Self::Error> {
        let mut errors = vec![];

        rayon::scope(|s| {
            s.spawn(|_| {
                errors = b
                    .transactions
                    .par_iter()
                    .filter_map(|t| {
                        let result = self.validate(t.as_slice());
                        if result.is_err() {
                            Some(result.unwrap_err())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Self::Error>>()
            })
        });
        Ok(())
    }
}
