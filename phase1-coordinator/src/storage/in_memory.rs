use crate::storage::Storage;

use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemory {
    storage: HashMap<String, String>,
}

impl Storage for InMemory {
    type Key = String;
    type Options = ();
    type Output = bool;
    type Value = String;

    /// Creates a new instance of storage.
    #[inline]
    fn new(_: Self::Options) -> Self {
        Self {
            storage: HashMap::default(),
        }
    }

    /// Returns the value for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.storage.get(key)
    }

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    #[inline]
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Self::Output {
        self.insert(key, value)
    }

    /// Removes a value from storage for a given key.
    #[inline]
    fn remove(&mut self, key: &Self::Key) -> Self::Output {
        self.remove(key)
    }
}
