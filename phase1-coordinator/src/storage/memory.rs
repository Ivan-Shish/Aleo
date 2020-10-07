use crate::{
    environment::Environment,
    storage::{Key, Locator, Storage, Value},
    CoordinatorError,
};

use itertools::Itertools;
use serde::{
    de::{self, Deserializer},
    ser::{self, Serializer},
    Deserialize,
    Serialize,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tracing::warn;

#[derive(Debug, Serialize, Deserialize)]
pub struct InMemoryEntry {
    key: Key,
    value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Memory {
    /// (Storage::Key, Storage::Value)
    storage: HashMap<Key, Value>,
    /// (round_height, (chunk_id, (contribution_id, verified))) // is_locked
    rounds: HashMap<u64, HashMap<u64, HashMap<u64, bool>>>,
    /// { round_heights }
    rounds_complete: HashSet<u64>,
}

impl Storage for Memory {
    /// Loads a new instance of `InMemory`.
    #[inline]
    fn load(_: &Environment) -> Result<Self, CoordinatorError>
    where
        Self: Sized,
    {
        Ok(Self {
            storage: HashMap::default(),
            rounds: HashMap::default(),
            rounds_complete: HashSet::default(),
        })
    }

    /// Returns the value reference for a given key from storage, if it exists.
    #[inline]
    fn get(&self, key: &Key) -> Option<Value> {
        match self.storage.get(key) {
            Some(key) => Some(key.clone()),
            None => None,
        }
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

impl Memory {
    /// Returns the transcript directory for a given round from the coordinator.
    fn round_directory(&self, round_height: u64) -> String {
        format!("round.{}", round_height)
    }

    /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        let round = self.round_directory(round_height);

        // Format the chunk directory as `{round_directory}/chunk.{chunk_id}`.
        format!("{}/chunk.{}", round, chunk_id)
    }

    /// Initializes the chunk directory for a given round height, and chunk ID.
    fn chunk_directory_init(&mut self, round_height: u64, chunk_id: u64) {
        // self.round_directory_init(round_height);
        if !self.rounds.contains_key(&round_height) {
            self.rounds.insert(round_height, HashMap::default());
        }

        if let Some(mut chunks) = self.rounds.get_mut(&round_height) {
            if !chunks.contains_key(&chunk_id) {
                chunks.insert(chunk_id, HashMap::default());
            }
        }
    }
}

impl Locator for Memory {
    // /// Initializes the round directory for a given round height.
    // fn round_directory_init(&mut self, round_height: u64) {
    //     if !self.rounds.contains_key(&round_height) {
    //         self.rounds.insert(round_height, HashMap::default());
    //     }
    // }

    // /// Returns `true` if the round directory for a given round height exists.
    // /// Otherwise, returns `false`.
    // fn round_directory_exists(&self, round_height: u64) -> bool {
    //     self.rounds.contains_key(&round_height)
    // }

    // /// Resets the round directory for a given round height.
    // fn round_directory_reset(&mut self, environment: &Environment, round_height: u64) {
    //     match environment {
    //         Environment::Test(_) => {
    //             self.rounds.remove(&round_height);
    //         }
    //         Environment::Development(_) => warn!("Coordinator is attempting to reset round storage in development"),
    //         Environment::Production(_) => warn!("Coordinator is attempting to reset round storage in production"),
    //     };
    // }

    // /// Resets the entire round directory.
    // fn round_directory_reset_all(&mut self, environment: &Environment) {
    //     match environment {
    //         Environment::Test(_) => self.rounds = HashMap::default(),
    //         Environment::Development(_) => warn!("Coordinator is attempting to reset round storage in development"),
    //         Environment::Production(_) => warn!("Coordinator is attempting to reset round storage in production"),
    //     };
    // }

    // /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
    // /// Otherwise, returns `false`.
    // fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool {
    //     // Check that the specified round exists.
    //     if let Some(chunks) = self.rounds.get(&round_height) {
    //         // Check that the specified chunk exists.
    //         if let Some(_) = chunks.get(&chunk_id) {
    //             return true;
    //         }
    //     }
    //     false
    // }

    /// Returns the contribution locator for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String {
        // Fetch the chunk directory path.
        let chunk = self.chunk_directory(round_height, chunk_id);

        // As the contribution at ID 0 is a continuation of the last contribution
        // in the previous round, it will always be verified by default.
        match verified || contribution_id == 0 {
            // Set the contribution locator as `{chunk_directory}/contribution.{contribution_id}.verified`.
            true => format!("{}/contribution.{}.verified", chunk, contribution_id),
            // Set the contribution locator as `{chunk_directory}/contribution.{contribution_id}.unverified`.
            false => format!("{}/contribution.{}.unverified", chunk, contribution_id),
        }
    }

    /// Initializes the contribution locator file for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator_init(&mut self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) {
        self.chunk_directory_init(round_height, chunk_id);

        if let Some(mut chunks) = self.rounds.get_mut(&round_height) {
            if let Some(mut contributions) = chunks.get_mut(&chunk_id) {
                if !contributions.contains_key(&contribution_id) {
                    contributions.insert(contribution_id, verified);
                }
            }
        }
    }

    /// Returns `true` if the contribution locator for a given round height, chunk ID,
    /// and contribution ID exists. Otherwise, returns `false`.
    fn contribution_locator_exists(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        verified: bool,
    ) -> bool {
        // Check that the specified round exists.
        if let Some(chunks) = self.rounds.get(&round_height) {
            // Check that the specified chunk exists.
            if let Some(contributions) = chunks.get(&chunk_id) {
                // Check that the specified contribution exists.
                if let Some(is_verified_file_type) = contributions.get(&contribution_id) {
                    // Check that the specified contribution file variant exists.
                    return verified == *is_verified_file_type;
                }
            }
        }
        false
    }

    /// Returns the round locator for a given round from the coordinator.
    fn round_locator(&self, round_height: u64) -> String {
        // Fetch the round directory path.
        let path = self.round_directory(round_height);

        // Format the round locator located at `{round_directory}/round`.
        format!("{}/round", path)
    }

    /// Returns `true` if the round locator for a given round height exists.
    /// Otherwise, returns `false`.
    fn round_locator_exists(&self, round_height: u64) -> bool {
        self.rounds_complete.contains(&round_height)
    }
}

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&match self {
            Key::RoundHeight => "roundheight".to_string(),
            Key::Round(round_height) => format!("round::{}", round_height),
            Key::Ping => "ping".to_string(),
            _ => return Err(ser::Error::custom("invalid serialization key")),
        })
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let (variant, data) = match s.splitn(2, "::").collect_tuple() {
            Some((variant, data)) => (variant, data),
            None => return Err(de::Error::custom("failed to parse serialization key")),
        };
        match (variant, data) {
            ("roundheight", "") => Ok(Key::RoundHeight),
            ("round", value) => Ok(Key::Round(u64::from_str(value).map_err(de::Error::custom)?)),
            ("ping", "") => Ok(Key::Ping),
            _ => Err(de::Error::custom("invalid deserialization key")),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&match self {
            Value::RoundHeight(round_height) => format!("roundheight::{}", round_height),
            Value::Round(round) => format!("round::{}", serde_json::to_string(round).map_err(ser::Error::custom)?),
            Value::Pong => "pong".to_string(),
            _ => return Err(ser::Error::custom("invalid serialization value")),
        })
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let (variant, data) = match s.splitn(2, "::").collect_tuple() {
            Some((variant, data)) => (variant, data),
            None => return Err(de::Error::custom("failed to parse deserialization value")),
        };
        match (variant, data) {
            ("roundheight", value) => Ok(Value::RoundHeight(u64::from_str(value).map_err(de::Error::custom)?)),
            ("round", value) => Ok(Value::Round(serde_json::from_str(value).map_err(de::Error::custom)?)),
            ("ping", "") => Ok(Value::Pong),
            _ => Err(de::Error::custom("invalid deserialization value")),
        }
    }
}

// impl Serialize for (Key, Value) {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&format!(
//             "{}/{}",
//             serde_json::to_string(&self.0),
//             serde_json::to_string(&self.1)
//         ))
//
//         // match self {
//         //     (Key::RoundHeight, Value::RoundHeight(round_height)) => {
//         //         format!("{}/{}", serde_json::to_string(key), serde_json::to_string(value))
//         //         // &json!({ "key": "roundheight", "value": format!("roundheight.{}", round_height) }).to_string()
//         //     }
//         //     (Key::Round(round_height), Value::Round(round)) => {
//         //         &json!({ "key": format!("round.{}", round_height), "value": format!("round.{}", &serde_json::to_string_pretty(round)) })
//         //             .to_string()
//         //     }
//         //     (Key::Ping, Value::Pong) => &json!({ "key": "ping", "value": "pong" }).to_string(),
//         //     _ => return Error::custom("invalid key value pair"),
//         // })
//     }
// }
//
// impl<'de> Deserialize<'de> for (Key, Value) {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         let [key, value] = s.splitn(2, '/').collect();
//         Ok((serde_json::from_str(key)?, serde_json::from_str(value)?))
//
//         // match s.as_str() {
//         //     "foo" => Enum::Foo,
//         //     "bar" => Enum::Bar,
//         //     "quux" => Enum::Quux,
//         //     _ => Enum::Other(s),
//         // })
//     }
// }
