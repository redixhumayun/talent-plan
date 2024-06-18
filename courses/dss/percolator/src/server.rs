use core::fmt;
use std::collections::BTreeMap;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

#[derive(Debug)]
enum ServerError {
    AlreadyLocked(Vec<u8>),
    NewerVersionAvailable(Vec<u8>),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ServerError::AlreadyLocked(ref key) => write!(
                f,
                "The key {} is already locked ",
                String::from_utf8_lossy(key)
            ),
            ServerError::NewerVersionAvailable(ref key) => write!(
                f,
                "The key {} has a newer version available",
                String::from_utf8_lossy(key)
            ),
        }
    }
}

impl std::error::Error for ServerError {}

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    counter: Arc<Mutex<AtomicU64>>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let counter_guard = self
            .counter
            .lock()
            .expect("error while acquiring lock on timestamp counter");
        let old_value = counter_guard.fetch_add(1, atomic::Ordering::SeqCst);
        Ok(TimestampResponse {
            timestamp: old_value,
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        unimplemented!()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        unimplemented!()
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        unimplemented!()
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        unimplemented!()
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        self.acquire_locks(req.kv_pairs, req.timestamp)
            .map_err(|e| {
                labrpc::Error::Other(format!(
                    "error while acquiring locks during prewrite: {}",
                    e
                ))
            })?;
        Ok(PrewriteResponse { res: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        unimplemented!()
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// This method will attempt to acquire a lock on each row and insert the data into the data column
    fn acquire_locks(&self, kv_pairs: Vec<KvPair>, start_ts: u64) -> Result<(), ServerError> {
        let mut primary_lock = None;
        for pair in kv_pairs.into_iter() {
            let mut storage = self.data.lock().unwrap();
            let is_locked = storage.read(pair.key.clone(), Column::Lock, Some(start_ts), None);
            if is_locked.is_some() {
                self.back_off_maybe_clean_up_lock(start_ts, pair.key.clone());
                return Err(ServerError::AlreadyLocked(pair.key));
            }
            storage.write(
                pair.key.clone(),
                Column::Lock,
                start_ts,
                Value::Timestamp(start_ts),
            );
            if primary_lock.is_none() {
                primary_lock = Some(pair.key.clone());
            }
            let is_newer_version_available =
                storage.read(pair.key.clone(), Column::Data, Some(start_ts), None);
            if is_newer_version_available.is_some() {
                self.back_off_maybe_clean_up_lock(start_ts, pair.key.clone());
                return Err(ServerError::NewerVersionAvailable(pair.key));
            }
            storage.write(pair.key, Column::Data, start_ts, Value::Vector(pair.value));
        }
        Ok(())
    }
}
