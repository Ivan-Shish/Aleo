use crate::{
    environment::Environment,
    storage::{Key, Storage, Value},
    CoordinatorError,
};

use evmap::shallow_copy::ShallowCopy;
use std::{
    collections::hash_map::RandomState,
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};

type ReadHandle = evmap::ReadHandle<Key, Value, (), RandomState>;
type WriteHandle = evmap::WriteHandle<Key, Value, (), RandomState>;

// lazy_static! {
//     static ref MAP: (Arc<Mutex<ReadHandle>>, Arc<Mutex<WriteHandle>>) = {
//         let (r, w) = evmap::new();
//         (Arc::new(Mutex::new(r)), Arc::new(Mutex::new(w)))
//     };
// }

impl ShallowCopy for Value {
    unsafe fn shallow_copy(&self) -> ManuallyDrop<Self> {
        match self {
            Value::RoundHeight(round_height) => ManuallyDrop::new(Value::RoundHeight(*round_height)),
            Value::Round(round) => ManuallyDrop::new(Value::Round(round.clone())),
            Value::Pong => ManuallyDrop::new(Value::Pong),
        }
    }
}

#[derive(Debug)]
pub struct ConcurrentMemory {
    reader: Arc<Mutex<ReadHandle>>,
    writer: Arc<Mutex<WriteHandle>>,
}

impl Storage for ConcurrentMemory {
    /// Loads a new instance of `ConcurrentMemory`.
    #[inline]
    fn load(_: &Environment) -> Result<Self, CoordinatorError> {
        // let reader = MAP.0.clone();
        // let writer = MAP.1.clone();

        let (r, w) = evmap::new();
        let reader = Arc::new(Mutex::new(r));
        let writer = Arc::new(Mutex::new(w));

        Ok(Self { reader, writer })
    }

    /// Returns the value reference for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Key) -> Option<Value> {
        let reader = self.reader.lock().unwrap();
        let result = match reader.get_one(key) {
            Some(value) => Some((*value).clone()),
            _ => None,
        };
        result
    }

    /// Returns `true` if a given key exists in storage. Otherwise, returns `false`.
    #[inline]
    fn contains_key(&self, key: &Key) -> bool {
        let reader = self.reader.lock().unwrap();
        reader.contains_key(key)
    }

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn insert(&mut self, key: Key, value: Value) -> bool {
        let reader = self.reader.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();
        writer.insert(key.clone(), value.clone());
        writer.refresh();
        let read = match reader.get_one(&key) {
            Some(read_value) => read_value,
            _ => return false,
        };
        *read == value
    }

    /// Removes a value from storage for a given key.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn remove(&mut self, key: &Key) -> bool {
        let reader = self.reader.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();
        writer.empty(key.clone());
        writer.refresh();
        let read = reader.get_one(&key);
        let result = read.is_none();
        result
    }
}
