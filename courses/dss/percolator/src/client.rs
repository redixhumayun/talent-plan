use std::time::Duration;

use futures::executor;
use labrpc::*;

use crate::{
    msg::{CommitRequest, KvPair, PrewriteRequest, TimestampRequest},
    service::{TSOClient, TransactionClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    transaction: Option<Transaction>,
}

#[derive(Clone)]
pub struct KVPair {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
pub struct Transaction {
    pub start_ts: u64,
    pub write_buffer: Vec<KVPair>,
}

impl Transaction {
    pub fn new(start_ts: u64) -> Self {
        Transaction {
            start_ts,
            write_buffer: Vec::new(),
        }
    }
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            transaction: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        let rpc = || self.tso_client.get_timestamp(&TimestampRequest {});
        match executor::block_on(self.call_with_retry(rpc)) {
            Ok(ts) => Ok(ts.timestamp),
            Err(e) => Err(e),
        }
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        let ts = self
            .get_timestamp()
            .expect("unable to get a timestamp from the oracle");
        let transaction = Transaction::new(ts);
        self.transaction = Some(transaction);
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        if let Some(transaction) = self.transaction.as_mut() {
            transaction.write_buffer.push(KVPair { key, value });
            return;
        }
        panic!("attempting to set a key value pair without a txn");
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        //  PRE-WRITE PHASE
        let transaction = self.transaction.as_ref().expect("transaction not found");
        let kv_pair = &transaction.write_buffer;
        let primary = kv_pair
            .first()
            .expect("cannot find the first key value pair");
        let secondaries = &kv_pair[1..];
        //  acquire a lock on the primary first
        let args = PrewriteRequest {
            timestamp: transaction.start_ts,
            kv_pair: Some(KvPair {
                key: primary.key.clone(),
                value: primary.value.clone(),
            }),
            primary: Some(KvPair {
                key: primary.key.clone(),
                value: primary.value.clone(),
            }),
        };
        let rpc = || self.txn_client.prewrite(&args);
        if executor::block_on(self.call_with_retry(rpc))?.res == false {
            return Ok(false);
        }
        //  acquire locks on the secondaries now
        for kv_pair in secondaries {
            let args = PrewriteRequest {
                timestamp: transaction.start_ts,
                kv_pair: Some(KvPair {
                    key: kv_pair.key.clone(),
                    value: kv_pair.value.clone(),
                }),
                primary: Some(KvPair {
                    key: primary.key.clone(),
                    value: primary.value.clone(),
                }),
            };
            let rpc = || self.txn_client.prewrite(&args);
            if executor::block_on(self.call_with_retry(rpc))?.res == false {
                return Ok(false);
            }
        }
        //  END PRE-WRITE PHASE

        //  COMMIT PHASE
        let commit_ts = self.get_timestamp()?;
        assert!(
            commit_ts > transaction.start_ts,
            "panic because the commit ts is not strictly greater than the start ts of the txn"
        );
        let args = CommitRequest {
            start_ts: transaction.start_ts,
            commit_ts,
            is_primary: true,
            kv_pair: Some(KvPair {
                key: primary.key.clone(),
                value: primary.value.clone(),
            }),
        };
        let rpc = || self.txn_client.commit(&args);
        if executor::block_on(self.call_with_retry(rpc))?.res == false {
            return Ok(false);
        }
        for kv_pair in secondaries {
            let args = CommitRequest {
                start_ts: transaction.start_ts,
                commit_ts,
                is_primary: false,
                kv_pair: Some(KvPair {
                    key: primary.key.clone(),
                    value: primary.value.clone(),
                }),
            };
            let rpc = || self.txn_client.commit(&args);
            if executor::block_on(self.call_with_retry(rpc))?.res == false {
                return Ok(false);
            }
        }
        //  END COMMIT PHASE
        Ok(true)
    }

    async fn call_with_retry<T>(
        &self,
        rpc: impl Fn() -> RpcFuture<labrpc::Result<T>>,
    ) -> labrpc::Result<T> {
        let mut attempts = 0;
        let mut delay = Duration::from_millis(BACKOFF_TIME_MS);

        for i in 0..RETRY_TIMES {
            let result = rpc().await;
            if result.is_ok() {
                return result;
            }
            std::thread::sleep(delay);
            delay = delay * 2;
        }
        return Err(labrpc::Error::Timeout);
    }
}
