use crate::{environment::Environment, objects::Round, CoordinatorError};

use serde::{Deserialize, Serialize};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A data structure representing all possible types of keys in storage.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Key {
    RoundHeight,
    Round(u64),
    Ping,
}

/// A data structure representing all possible types of values in storage.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Value {
    RoundHeight(u64),
    Round(Round),
    Pong,
}

pub type StorageReader<'a> = RwLockReadGuard<'a, Box<dyn Storage>>;
pub type StorageWriter<'a> = RwLockWriteGuard<'a, Box<dyn Storage>>;

/// A standard model for storage.
pub trait Storage: Send + Sync + Locator {
    /// Loads a new instance of `Storage`.
    fn load(environment: &Environment) -> Result<Self, CoordinatorError>
    where
        Self: Sized;

    // fn locate(&self) -> dyn Locator;

    /// Returns the value for a given key from storage, if it exists.
    fn get(&self, key: &Key) -> Option<Value>;

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

pub trait Locator {
    // /// Returns the round directory for a given round height from the coordinator.
    // fn round_directory(&self, round_height: u64) -> String;

    // /// Initializes the round directory for a given round height.
    // fn round_directory_init(&mut self, round_height: u64);

    // /// Returns `true` if the round directory for a given round height exists.
    // /// Otherwise, returns `false`.
    // fn round_directory_exists(&self, round_height: u64) -> bool;

    // /// Resets the round directory for a given round height.
    // fn round_directory_reset(&mut self, environment: &Environment, round_height: u64);

    // /// Resets the entire round directory.
    // fn round_directory_reset_all(&mut self, environment: &Environment);

    // /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    // fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String;

    // /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
    // /// Otherwise, returns `false`.
    // fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool;

    /// Returns the contribution locator for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String;

    /// Initializes the contribution locator file for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator_init(&mut self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool);

    /// Returns `true` if the contribution locator for a given round height, chunk ID,
    /// and contribution ID exists. Otherwise, returns `false`.
    fn contribution_locator_exists(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        verified: bool,
    ) -> bool;

    /// Returns the round locator for a given round from the coordinator.
    fn round_locator(&self, round_height: u64) -> String;

    /// Returns `true` if the round locator for a given round height exists.
    /// Otherwise, returns `false`.
    fn round_locator_exists(&self, round_height: u64) -> bool;
}
