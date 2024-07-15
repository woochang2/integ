use dashmap::DashMap;
use ethers_core::{
    rand::{self, distributions::Uniform, prelude::Distribution, prelude::*},
    types::{
        transaction::eip2718::TypedTransaction, Address, Signature, TransactionRequest, H160, H256,
        U256,
    },
    utils::{hex, rlp},
};
use ethers_providers::{Http, Provider};
use ethers_signers::{LocalWallet, Signer};
use narwhal_types::{Empty, TransactionProto, TransactionsClient};
use rand_distr::Zipf;
use sha3::{Digest, Keccak256};
use std::{str::FromStr, sync::Arc};
use sui_network::tonic::{self, transport::Channel};
use tracing::info;

use crate::workloads::smallbank::contract::SmallBank;
// use crate::SMALLBANK_BYTECODE;

pub const CONTRACT_BYTECODE: &str = include_str!("../../contracts/SmallBank.bin");
pub const ADMIN_SECRET_KEY: &[u8] = &[
    95 as u8, 126, 251, 131, 73, 90, 235, 201, 21, 22, 203, 137, 149, 240, 205, 60, 221, 27, 81,
    53, 2, 200, 90, 185, 25, 240, 166, 21, 177, 41, 49, 254,
];
// pub const ADMIN_ADDRESS: &str = "0xe14de1592b52481b94b99df4e9653654e14fffb6";
#[allow(dead_code)]
pub const DEFAULT_CONTRACT_ADDRESS: &str = "0x1000000000000000000000000000000000000000";

pub enum SmallBankTransactionType {
    AMALGAMATE,
    GetBalance,
    SendPayment,
    UpdateBalance,
    UpdateSaving,
    WriteCheck,
}

impl SmallBankTransactionType {
    pub fn from(value: u32) -> SmallBankTransactionType {
        match value {
            0 => SmallBankTransactionType::AMALGAMATE,
            1 => SmallBankTransactionType::GetBalance,
            2 => SmallBankTransactionType::SendPayment,
            3 => SmallBankTransactionType::UpdateBalance,
            4 => SmallBankTransactionType::UpdateSaving,
            5 => SmallBankTransactionType::WriteCheck,
            _ => panic!("Invalid transaction type"),
        }
    }
}

#[derive(Clone)]
pub struct SmallBankTransactionHandler {
    op_gen: Uniform<u32>,
    nonce_gen: Uniform<u64>,
    zipfian_acc_gen: Zipf<f32>,
    uniform_bal_gen: Uniform<u32>,
    admin_wallet: LocalWallet,
    provider: Provider<Http>,
    chain_id: u64,
    narwhal_client: TransactionsClient<Channel>,
    contract: Option<SmallBank<Provider<Http>>>,
    users: DashMap<Address, (LocalWallet, u32)>,
}

impl SmallBankTransactionHandler {
    pub fn new(
        provider: Provider<Http>,
        narwhal_client: TransactionsClient<Channel>,
        chain_id: u64,
        skewness: f32,
    ) -> SmallBankTransactionHandler {
        let nonce_gen = Uniform::new(u64::MIN, u64::MAX);

        info!(
            "contract address: {}",
            H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap()
        );
        SmallBankTransactionHandler {
            op_gen: Uniform::new(0, 6),
            nonce_gen,
            zipfian_acc_gen: Zipf::new(100_000, skewness).unwrap(),
            uniform_bal_gen: Uniform::new(1, 10),
            admin_wallet: LocalWallet::from_bytes(ADMIN_SECRET_KEY.try_into().unwrap())
                .unwrap()
                .with_chain_id(chain_id),
            provider: provider.clone(),
            chain_id,
            narwhal_client,
            contract: Some(SmallBank::new(
                H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap(),
                Arc::new(provider),
            )),
            users: DashMap::default(),
        }
    }

    #[allow(dead_code)]
    pub async fn init(&mut self) -> Result<tonic::Response<Empty>, tonic::Status> {
        info!("Init smallbank transaction handler");
        info!("admin address: {:?}", self.admin_wallet.address());

        self.contract = Some(SmallBank::new(
            self.create_contract_address(),
            Arc::new(self.provider.clone()),
        ));
        self.deploy_contract().await
    }

    #[allow(dead_code)]
    async fn submit_transaction(
        &mut self,
        tx_request: TransactionRequest,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let tx: TypedTransaction = From::<TransactionRequest>::from(tx_request);
        let raw_tx = self.get_signed(tx);

        // NOTE: This log entry is used to compute performance.
        let tx_id = u64::from_be_bytes(raw_tx[2..10].try_into().unwrap());
        info!("Sending sample transaction {tx_id}");

        self.narwhal_client
            .submit_transaction(TransactionProto {
                transaction: raw_tx,
            })
            .await
    }

    #[allow(dead_code)]
    async fn deploy_contract(&mut self) -> Result<tonic::Response<Empty>, tonic::Status> {
        info!("Deploy contract");

        let tx_request = TransactionRequest::default()
            .from(self.admin_wallet.address())
            .chain_id(self.chain_id)
            // .value(1_000_000 as i32)
            .gas(1_000_000u64)
            .data(hex::decode(CONTRACT_BYTECODE).unwrap())
            .nonce(U256::one());

        self.submit_transaction(tx_request).await
    }

    #[allow(dead_code)]
    fn create_contract_address(&mut self) -> Address {
        let mut stream = rlp::RlpStream::new_list(2);
        stream.append(&self.admin_wallet.address());
        stream.append(&U256::zero());
        H256::from_slice(Keccak256::digest(&stream.out()).as_slice()).into()
    }

    pub fn create_random_request(&self) -> bytes::Bytes {
        self.create_request(self.get_random_op())
    }

    fn create_request(&self, ops: SmallBankTransactionType) -> bytes::Bytes {
        let call_data: ethers::types::Bytes = match ops {
            SmallBankTransactionType::AMALGAMATE => self
                .contract
                .as_ref()
                .unwrap()
                .amalgamate(self.get_random_account_id(), self.get_random_account_id())
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
            SmallBankTransactionType::GetBalance => self
                .contract
                .as_ref()
                .unwrap()
                .get_balance(self.get_random_account_id())
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
            SmallBankTransactionType::SendPayment => self
                .contract
                .as_ref()
                .unwrap()
                .send_payment(
                    self.get_random_account_id(),
                    self.get_random_account_id(),
                    self.get_random_balance(),
                )
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
            SmallBankTransactionType::UpdateBalance => self
                .contract
                .as_ref()
                .unwrap()
                .deposit_checking(self.get_random_account_id(), self.get_random_balance())
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
            SmallBankTransactionType::UpdateSaving => self
                .contract
                .as_ref()
                .unwrap()
                .update_saving(self.get_random_account_id(), self.get_random_balance())
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
            SmallBankTransactionType::WriteCheck => self
                .contract
                .as_ref()
                .unwrap()
                .write_check(self.get_random_account_id(), self.get_random_balance())
                .tx
                .data()
                .cloned()
                .unwrap_or_default(),
        };

        let mut tx = TypedTransaction::Legacy(Default::default());

        tx.set_from(self.admin_wallet.address())
            .set_to(H160::from_str(DEFAULT_CONTRACT_ADDRESS).unwrap())
            .set_chain_id(self.chain_id)
            .set_gas(100_000u64)
            .set_data(call_data)
            .set_gas_price(0u64)
            .set_access_list(Default::default()); // noop

        self.get_signed(tx)
    }

    fn get_random_op(&self) -> SmallBankTransactionType {
        SmallBankTransactionType::from(self.op_gen.sample(&mut rand::thread_rng()))
    }

    fn get_random_account_id(&self) -> String {
        rand::thread_rng().sample(self.zipfian_acc_gen).to_string()
    }

    fn get_random_balance(&self) -> U256 {
        U256::from(self.uniform_bal_gen.sample(&mut rand::thread_rng()))
    }

    fn get_random_user(&self) -> (LocalWallet, u32) {
        let random_user = LocalWallet::new(&mut rand::thread_rng()).with_chain_id(self.chain_id);
        let addr = random_user.address();

        match self.users.entry(addr) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let (_wallet, nonce) = entry.get_mut();
                *nonce += 1;
                (random_user, nonce.clone() - 1)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert((random_user.clone(), 0u32));
                (random_user, 0u32)
            }
        }
    }

    fn get_signed(&self, mut tx: TypedTransaction) -> bytes::Bytes {
        let (user, next_nonce) = self.get_random_user();
        tx.set_nonce(next_nonce);

        let signature: Signature = user.sign_transaction_sync(&tx).expect("signature failed");
        tx.rlp_signed(&signature).0
    }
}
