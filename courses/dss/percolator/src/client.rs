use std::time::Duration;

use futures::executor;
use labrpc::*;

use crate::{
    msg::TimestampRequest,
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
        let mut attempts = 0;
        let mut delay = Duration::from_millis(BACKOFF_TIME_MS);

        let mut get_ts = || loop {
            match executor::block_on(self.tso_client.get_timestamp(&TimestampRequest {})) {
                Ok(ts) => return Ok(ts.timestamp),
                Err(e) => {
                    attempts += 1;
                    if attempts >= RETRY_TIMES {
                        return Err(e);
                    }
                    std::thread::sleep(delay);
                    delay = delay * 2;
                }
            }
        };
        get_ts()
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
        // Your code here.
        unimplemented!()
    }
}
