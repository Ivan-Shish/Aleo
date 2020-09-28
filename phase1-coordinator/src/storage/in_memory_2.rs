use crate::{
    storage::{Key, Storage, Value},
    CoordinatorError,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemory2 {
    storage: HashMap<Key, Value>,
}

impl Storage for InMemory2 {
    /// Loads a new instance of `InMemory`.
    #[inline]
    fn load() -> Result<Self, CoordinatorError> {
        // Initialize the data structure for storage.
        // let mut storage = HashMap::default();

        // Load all rounds from transcript into storage.
        // for chunk_id in 0..environment.number_of_chunks() {
        //     // Open the transcript file for round 0.
        //     let transcript = environment.transcript_locator(0, chunk_id);
        //     let file = OpenOptions::new().read(true).open(&transcript)?;
        //     let reader = unsafe { MmapOptions::new().map(&file)? };
        // }

        Ok(Self {
            storage: HashMap::default(),
        })
    }

    /// Stores an instance of `InMemory`.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn save(&mut self) -> bool {
        // As this storage is in memory, we can always return `true`.
        true
    }

    /// Returns the value reference for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Key) -> Option<&Value> {
        self.storage.get(key)
    }

    /// Returns the mutable value reference for a given key from storage, if it exists.
    #[inline]
    fn get_mut(&mut self, key: &Key) -> Option<&mut Value> {
        self.storage.get_mut(key)
    }

    /// Returns `true` if a given key exists in storage. Otherwise, returns `false`.
    #[inline]
    fn contains_key(&self, key: &Key) -> bool {
        self.storage.contains_key(key)
    }

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn insert(&mut self, key: Key, value: Value) -> bool {
        self.storage.insert(key, value);
        true
    }

    /// Removes a value from storage for a given key.
    /// If successful, returns `true`. Otherwise, returns `false`.
    #[inline]
    fn remove(&mut self, key: &Key) -> bool {
        self.storage.remove(key).is_some()
    }
}
