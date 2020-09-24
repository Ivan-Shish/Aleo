use crate::objects::Round;

use serde::{Deserialize, Serialize};

#[derive(Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum Key {
    RoundHeight,
    Round(u64),
    Ping,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    RoundHeight(u64),
    Round(Round),
    Pong,
}

/// A standard model for storage.
pub trait Storage: Sized + Send + Sync {
    /// Returns the value reference for a given key from storage, if it exists.
    fn get(&self, key: &Key) -> Option<&Value>;

    /// Returns the mutable value reference for a given key from storage, if it exists.
    fn get_mut(&mut self, key: &Key) -> Option<&mut Value>;

    /// Inserts a new key value pair into storage,
    /// updating the current value for a given key if it exists.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn insert(&mut self, key: Key, value: Value) -> bool;

    /// Removes a value from storage for a given key.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn remove(&mut self, key: &Key) -> bool;

    /// Loads a new instance of `Storage`.
    fn load() -> Self;

    /// Stores an instance of `Storage`.
    /// If successful, returns `true`. Otherwise, returns `false`.
    fn store(&mut self) -> bool;
}
