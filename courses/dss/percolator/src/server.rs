use core::fmt;
use std::collections::BTreeMap;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

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

#[derive(Clone, PartialEq, Debug)]
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
    // Reads the first key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read_first(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(Key, Value)> {
        let col = match column {
            Column::Data => &self.data,
            Column::Lock => &self.lock,
            Column::Write => &self.write,
        };

        for ((k, ts), value) in col.iter() {
            if k == &key
                && ts_start_inclusive.map_or(true, |start| *ts >= start)
                && ts_end_inclusive.map_or(true, |end| *ts <= end)
            {
                return Some(((k.clone(), *ts), value.clone()));
            }
        }
        None
    }

    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(Key, Value)> {
        let col = match column {
            Column::Data => &self.data,
            Column::Lock => &self.lock,
            Column::Write => &self.write,
        };
        let mut res = None;
        let mut max_timestamp_seen = 0;

        for ((k, ts), value) in col.iter() {
            if k == &key
                && ts_start_inclusive.map_or(true, |start| *ts >= start)
                && ts_end_inclusive.map_or(true, |end| *ts <= end)
                && *ts >= max_timestamp_seen
            {
                max_timestamp_seen = *ts;
                res = Some(((k.clone(), *ts), value.clone()));
            }
        }
        res
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let col = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };
        col.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let col = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };
        let mut keys_to_remove = Vec::new();
        for ((k, ts), _) in col.iter() {
            if k == &key && *ts == commit_ts {
                keys_to_remove.push((k.clone(), *ts));
            }
        }
        for key in keys_to_remove {
            col.remove(&key);
        }
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
        let start_ts = req.timestamp;
        let key = req.key;
        loop {
            let mut storage = self.data.lock().unwrap();
            let is_locked = storage.read(key.clone(), Column::Lock, Some(0), Some(start_ts));
            if let Some(((conflicting_key, conflicting_ts), conflicting_value)) = is_locked {
                if let Value::Vector(primary_key) = conflicting_value {
                    let primary_lock =
                        //  should the upper end be the current txn's timestamp?
                        storage.read(primary_key.clone(), Column::Lock, Some(0), None);
                    if primary_lock.is_none() {
                        let latest_pk_write =
                            storage.read(primary_key, Column::Write, Some(conflicting_ts), None);
                        storage.erase(conflicting_key.clone(), Column::Lock, conflicting_ts);
                        if latest_pk_write.is_none() {
                            break;
                        }
                        let latest_pk_write = latest_pk_write.unwrap();
                        storage.write(
                            conflicting_key,
                            Column::Write,
                            latest_pk_write.0 .1,
                            Value::Timestamp(conflicting_ts),
                        );
                    }
                }
            }
        }
        unimplemented!()
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let primary = req.primary.ok_or_else(|| {
            labrpc::Error::Other("primary kv_pair is missing in the prewrite request".to_string())
        })?;
        let kv_pair = req.kv_pair.ok_or_else(|| {
            labrpc::Error::Other("kv_pair is missing in the prewrite request".to_string())
        })?;
        let mut storage = self.data.lock().unwrap();
        let new_data = storage.read(kv_pair.key.clone(), Column::Data, Some(req.timestamp), None);
        if new_data.is_some() {
            return Ok(PrewriteResponse { res: false });
        }
        let is_locked = storage.read(kv_pair.key.clone(), Column::Lock, Some(0), None);
        if is_locked.is_some() {
            return Ok(PrewriteResponse { res: false });
        }
        //  all checks completed, place data and lock
        storage.write(
            kv_pair.key.clone(),
            Column::Data,
            req.timestamp,
            Value::Vector(kv_pair.value.clone()),
        );
        if primary == kv_pair {
            storage.write(
                kv_pair.key.clone(),
                Column::Lock,
                req.timestamp,
                Value::Timestamp(req.timestamp),
            );
        } else {
            storage.write(
                kv_pair.key.clone(),
                Column::Lock,
                req.timestamp,
                Value::Vector(kv_pair.key),
            );
        }
        Ok(PrewriteResponse { res: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut storage = self.data.lock().unwrap();
        let kv_pair = req
            .kv_pair
            .expect("kv_pair is missing in the commit request");
        if req.is_primary {
            //  check lock on primary still holds
            let is_locked = storage.read(
                kv_pair.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            );
            if is_locked.is_none() {
                return Ok(CommitResponse { res: false });
            }
        }

        //  create write and remove lock
        storage.write(
            kv_pair.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        storage.erase(kv_pair.key, Column::Lock, req.start_ts);
        Ok(CommitResponse { res: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}
