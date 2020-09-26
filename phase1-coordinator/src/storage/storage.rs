use crate::{objects::Round, CoordinatorError};

use serde::{Deserialize, Serialize};

/// A data structure representing all possible types of keys in storage.
#[derive(Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum Key {
    RoundHeight,
    Round(u64),
    Ping,
}

/// A data structure representing all possible types of values in storage.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    RoundHeight(u64),
    Round(Round),
    Pong,
}

/// A standard model for storage.
pub trait Storage: Sized + Send + Sync {
    /// Loads a new instance of `Storage`.
    fn load() -> Result<Self, CoordinatorError>;

    /// Stores an instance of `Storage`.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn save(&mut self) -> bool;

    /// Returns the value reference for a given key from storage, if it exists.
    fn get(&self, key: &Key) -> Option<&Value>;

    /// Returns the mutable value reference for a given key from storage, if it exists.
    fn get_mut(&mut self, key: &Key) -> Option<&mut Value>;

    /// Returns `true` if a given key exists in storage. Otherwise, returns `false`.
    fn contains_key(&self, key: &Key) -> bool;

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn insert(&mut self, key: Key, value: Value) -> bool;

    /// Removes a value from storage for a given key.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn remove(&mut self, key: &Key) -> bool;
}
