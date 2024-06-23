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

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
    LockPlacedAt(SystemTime),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Timestamp(ts) => write!(f, "Timestamp({})", ts),
            Value::Vector(bytes) => write!(f, "Vector({:?})", String::from_utf8_lossy(bytes)),
            Value::LockPlacedAt(time) => write!(f, "LockPlacedAt({:?})", time),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::Vector(bytes) => write!(f, "{:?}", String::from_utf8_lossy(bytes)),
            Value::LockPlacedAt(time) => write!(f, "{:?}", time),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueError {
    IncorrectVariant,
}

impl Value {
    pub fn as_lock_placed_at(&self) -> Result<SystemTime, ValueError> {
        match self {
            Value::LockPlacedAt(time) => Ok(*time),
            _ => Err(ValueError::IncorrectVariant),
        }
    }
    pub fn as_vector(&self) -> Result<Vec<u8>, ValueError> {
        match self {
            Value::Vector(key) => Ok(key.clone()),
            _ => Err(ValueError::IncorrectVariant),
        }
    }
    pub fn as_timestamp(&self) -> Result<u64, ValueError> {
        match self {
            Value::Timestamp(ts) => Ok(*ts),
            _ => Err(ValueError::IncorrectVariant),
        }
    }
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

impl std::fmt::Display for KvTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        // Header
        writeln!(
            f,
            "{:<20} {:<20} {:<20} {:<20}",
            "Key", "Data", "Lock", "Write"
        )?;
        // Separator
        writeln!(f, "{:-<1$}", "", 80)?;

        // Collect and sort all unique keys from the three maps
        let mut all_keys = self
            .write
            .keys()
            .chain(self.data.keys())
            .chain(self.lock.keys())
            .collect::<Vec<&Key>>();
        all_keys.sort();
        all_keys.reverse();
        all_keys.dedup();

        // Iterate over the keys and print values from each map
        for key in all_keys {
            let data_val: String = self.data.get(key).map_or("".to_string(), |v| {
                format!(
                    "({},{}), {}",
                    String::from_utf8_lossy(&key.0),
                    key.1,
                    v.to_string()
                )
            });
            let lock_val: String = self.lock.get(key).map_or("".to_string(), |v| {
                format!(
                    "({},{}), {}",
                    String::from_utf8_lossy(&key.0),
                    key.1,
                    v.to_string()
                )
            });
            let write_val: String = self.write.get(key).map_or("".to_string(), |v| {
                format!(
                    "({},{}), {}",
                    String::from_utf8_lossy(&key.0),
                    key.1,
                    v.to_string()
                )
            });
            let formatted_key = format!("{},{}", String::from_utf8_lossy(&key.0), key.1);
            writeln!(
                f,
                "{:<20} {:<20} {:<20} {:<20}",
                formatted_key, data_val, lock_val, write_val
            )?;
        }

        Ok(())
    }
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
            let value = col.remove(&key);
            assert!(value.is_some());
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
        loop {
            let mut storage = self.data.lock().unwrap();
            let is_row_locked =
                storage.read(req.key.clone(), Column::Lock, Some(0), Some(req.timestamp));
            if is_row_locked.is_some() {
                drop(storage);
                self.back_off_maybe_clean_up_lock(req.timestamp, req.key.clone());
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }

            let start_ts =
                match storage.read(req.key.clone(), Column::Write, Some(0), Some(req.timestamp)) {
                    Some(((_, commit_ts), value)) => match value {
                        Value::Timestamp(start_ts) => start_ts,
                        _ => {
                            return Err(labrpc::Error::Other(format!(
                                "unexpected value found in write columnf or key {:?} at ts {}",
                                req.key, commit_ts
                            )))
                        }
                    },
                    None => {
                        return Ok(GetResponse {
                            success: false,
                            value: Vec::new(),
                        });
                    }
                };

            let data = match storage.read(
                req.key.clone(),
                Column::Data,
                Some(start_ts),
                Some(start_ts),
            ) {
                Some(((_, _), value)) => match value {
                    Value::Vector(bytes) => bytes,
                    _ => {
                        return Err(labrpc::Error::Other(format!(
                            "unexpected value found in data column for key {:?} at ts {}",
                            req.key, start_ts
                        )));
                    }
                },
                None => {
                    return Err(labrpc::Error::Other(format!(
                        "No value found in data column for key {:?} at timestamp {}",
                        req.key, start_ts
                    )));
                }
            };

            return Ok(GetResponse {
                success: true,
                value: data,
            });
        }
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
        match storage.read(
            kv_pair.key.clone(),
            Column::Write,
            Some(req.timestamp),
            None,
        ) {
            Some(_) => return Ok(PrewriteResponse { res: false }),
            None => (),
        };
        match storage.read(kv_pair.key.clone(), Column::Lock, Some(0), None) {
            Some(_) => return Ok(PrewriteResponse { res: false }),
            None => (),
        };
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
                Value::LockPlacedAt(SystemTime::now()),
            );
        } else {
            storage.write(
                kv_pair.key.clone(),
                Column::Lock,
                req.timestamp,
                Value::Vector(primary.key),
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
            match storage.read(
                kv_pair.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            ) {
                Some(_) => (),
                None => {
                    return Ok(CommitResponse { res: false });
                }
            };
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
        //  STEPS:
        //  1. Recheck the condition that prompted this call by re-acquiring lock. Things might have changed
        //  2. Check if the lock is the primary lock. If secondary lock, get primary lock
        //  3. If primary lock present and
        //      a. has expired, roll-back the txn
        //      b. has not expired, do nothing and retry after some time
        //  4. If primary lock not present and
        //      a. Data found in Write column, roll-forward the txn
        //      b. No data found in Write column, remove stale lock

        let mut storage = self.data.lock().unwrap();
        let ((key, start_ts), value) =
            match storage.read(key.clone(), Column::Lock, Some(0), Some(start_ts)) {
                Some((key, value)) => (key, value),
                None => return,
            };

        let is_primary_lock = match value {
            Value::LockPlacedAt(creation_time) => true,
            Value::Vector(ref data) => false,
            Value::Timestamp(_) => panic!(
                "unexpected value of bytes found in lock column, expected SystemTime or Vec<u8>"
            ),
        };

        if is_primary_lock {
            if self.check_if_primary_lock_expired(value) {
                self.remove_lock_and_rollback(&mut storage, key, start_ts);
            }
            return;
        }

        //  handle the secondary lock here
        let primary_key = value
            .as_vector()
            .expect("unexpected value in lock column, expected Vec<u8>");
        match storage.read(primary_key.clone(), Column::Lock, Some(0), Some(start_ts)) {
            Some(((_, conflicting_start_ts), value)) => {
                if self.check_if_primary_lock_expired(value) {
                    self.remove_lock_and_rollback(&mut storage, primary_key, conflicting_start_ts);
                    self.remove_lock_and_rollback(&mut storage, key, start_ts);
                    return;
                }
            }
            None => {
                //  the primary lock is gone, check for data
                match storage.read(primary_key, Column::Write, None, None) {
                    None => {
                        self.remove_lock_and_rollback(&mut storage, key, start_ts);
                    }
                    Some(((_, commit_ts), value)) => {
                        let start_ts = value
                            .as_timestamp()
                            .expect("unexpected value in write column, expected ts");
                        self.remove_lock_and_roll_forward(&mut storage, key, start_ts, commit_ts);
                    }
                }
            }
        }
    }

    fn check_if_primary_lock_expired(&self, value: Value) -> bool {
        let lock_creation_time = value
            .as_lock_placed_at()
            .expect("unexpected value in lock column, expected SystemTime");
        let ttl_duration = Duration::from_nanos(TTL);
        let future_time = lock_creation_time + ttl_duration;
        future_time < SystemTime::now()
    }

    fn remove_lock_and_rollback(
        &self,
        storage: &mut std::sync::MutexGuard<KvTable>,
        key: Vec<u8>,
        timestamp: u64,
    ) {
        storage.erase(key.clone(), Column::Lock, timestamp);
        storage.erase(key.clone(), Column::Data, timestamp);
    }

    fn remove_lock_and_roll_forward(
        &self,
        storage: &mut std::sync::MutexGuard<KvTable>,
        key: Vec<u8>,
        start_ts: u64,
        commit_ts: u64,
    ) {
        storage.erase(key.clone(), Column::Lock, start_ts);
        storage.write(key, Column::Write, commit_ts, Value::Timestamp(start_ts));
    }
}
