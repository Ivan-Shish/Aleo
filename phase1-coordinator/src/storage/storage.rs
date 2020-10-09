use crate::{environment::Environment, objects::Round, CoordinatorError};
use phase1::helpers::CurveKind;

use memmap::{Mmap, MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    io::{BufReader, BufWriter, Read, Write},
    sync::{RwLockReadGuard, RwLockWriteGuard},
};
use zexe_algebra::{Bls12_377, BW6_761};

/// A data structure representing all possible types of keys in storage.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Locator
// where
//     Self: fmt::Display,
{
    RoundHeight,
    RoundState(u64),
    RoundFile(u64),
    ContributionFile(u64, u64, u64, bool),
}

// impl StorageLocator for Locator {
//     fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError> {
//
//     }
// }

/// A data structure representing all possible types of values in storage.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Object
// where
//     Self: AsRef<[u8]>, // + AsMut<[u8]>,
{
    RoundHeight(u64),
    RoundState(Round),
    RoundFile(Vec<u8>),
    ContributionFile(Vec<u8>),
}

impl Object {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Object::RoundHeight(height) => serde_json::to_vec(height).expect("round height to bytes failed"),
            Object::RoundState(round) => serde_json::to_vec_pretty(round).expect("round state to bytes failed"),
            Object::RoundFile(round) => round.to_vec(),
            Object::ContributionFile(contribution) => contribution.to_vec(),
        }
    }

    /// Returns the size in bytes of the object.
    pub fn size(&self) -> u64 {
        match self {
            Object::RoundHeight(round_height) => self.to_bytes().len() as u64,
            Object::RoundState(round) => self.to_bytes().len() as u64,
            Object::RoundFile(round) => round.len() as u64,
            Object::ContributionFile(contribution) => contribution.len() as u64,
        }
    }

    /// Returns the expected file size of an aggregated round.
    pub fn round_file_size(environment: &Environment, round_height: u64) -> u64 {
        let compressed = environment.compressed_inputs();
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        match curve {
            CurveKind::Bls12_377 => round_filesize!(Bls12_377, settings, round_height, compressed),
            CurveKind::BW6 => round_filesize!(BW6_761, settings, round_height, compressed),
        }
    }

    /// Returns the expected file size of a chunked contribution.
    pub fn contribution_file_size(environment: &Environment, chunk_id: u64, verified: bool) -> u64 {
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        let compressed = match verified {
            // The verified contribution file is used as *input* in the next computation.
            true => environment.compressed_inputs(),
            // The unverified contribution file the *output* of the current computation.
            false => environment.compressed_outputs(),
        };
        match (curve, verified) {
            (CurveKind::Bls12_377, true) => verified_contribution_size!(Bls12_377, settings, chunk_id, compressed),
            (CurveKind::Bls12_377, false) => unverified_contribution_size!(Bls12_377, settings, chunk_id, compressed),
            (CurveKind::BW6, true) => verified_contribution_size!(BW6_761, settings, chunk_id, compressed),
            (CurveKind::BW6, false) => unverified_contribution_size!(BW6_761, settings, chunk_id, compressed),
        }
    }
}

// impl AsRef<[u8]> for Object {
//     fn as_ref(&self) -> &[u8] {
//         match self {
//             Object::RoundHeight(round_height) => {
//                 // let buffer = serde_json::to_vec(round_height).expect("round state serialization failed");
//                 // &buffer[..]
//                 let buffer = serde_json::to_vec(round_height).expect("round state serialization failed");
//                 &buffer.to_owned()
//             }
//             Object::RoundState(round) => {
//                 let buffer = serde_json::to_vec(round).expect("round state serialization failed");
//                 &buffer.to_owned()
//
//                 // &buffer[..]
//                 // &serde_json::to_vec(round).expect("round state serialization failed")
//             }
//             Object::RoundFile(round) => round,
//             Object::ContributionFile(contribution) => contribution,
//         }
//     }
// }

// impl AsMut<[u8]> for Object {
//     fn as_mut(&mut self) -> &mut [u8] {
//         match self {
//             Object::RoundHeight(round_height) => {
//                 &mut serde_json::to_vec(round_height).expect("round state serialization failed")
//             }
//             Object::RoundState(round) => &mut serde_json::to_vec(round).expect("round state serialization failed"),
//             Object::RoundFile(mut round) => &mut round,
//             Object::ContributionFile(mut contribution) => &mut contribution,
//         }
//     }
// }

// impl Serialize for Object {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&match self {
//             Object::RoundHeight(round_height) => format!("roundheight::{}", round_height),
//             Object::Round(round) => format!("round::{}", serde_json::to_string(round).map_err(ser::Error::custom)?),
//             // Object::Pong => "pong".to_string(),
//             _ => return Err(ser::Error::custom("invalid serialization value")),
//         })
//     }
// }
//
// impl<'de> Deserialize<'de> for Object {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         let (variant, data) = match s.splitn(2, "::").collect_tuple() {
//             Some((variant, data)) => (variant, data),
//             None => return Err(de::Error::custom("failed to parse deserialization value")),
//         };
//         match (variant, data) {
//             ("roundheight", value) => Ok(Object::RoundHeight(u64::from_str(value).map_err(de::Error::custom)?)),
//             ("round", value) => Ok(Object::Round(serde_json::from_str(value).map_err(de::Error::custom)?)),
//             // ("ping", "") => Ok(Object::Pong),
//             _ => Err(de::Error::custom("invalid deserialization value")),
//         }
//     }
// }

pub type StorageReadOnly<'a> = RwLockReadGuard<'a, Box<dyn Storage>>;
pub type StorageWrite<'a> = RwLockWriteGuard<'a, Box<dyn Storage>>;

pub trait ObjectReadGuard: AsRef<[u8]> {}

// pub type ObjectReader<'a> = RwLockReadGuard<'a, dyn AsRef<[u8]>>;

// TODO (howardwu): Genericize this if necessary for remote objects.
//  Alternative, usage of temporary memory-backed local files can also work.
pub type ObjectReader<'a> = RwLockReadGuard<'a, MmapMut>;
pub type ObjectWriter<'a> = RwLockWriteGuard<'a, MmapMut>;

// pub type ObjectReader<'a> = Box<dyn AsRef<[u8]>>;

// pub struct ObjectReader<'a>(pub(super) RwLockReadGuard<'a>, pub(super) Object);

// pub type ObjectWriter<'a> = &'a mut [u8];

// pub struct ObjectReader<'a> {
//     pub(crate) lock: RwLockReadGuard<'a, dyn Read>,
//     pub(crate) data: Object,
// }
//
// pub struct ObjectWriter<'a> {
//     pub(crate) lock: RwLockWriteGuard<'a, dyn Write>,
//     pub(crate) data: Object,
// }

/// A standard model for storage.
pub trait Storage: Send + Sync + StorageLocator + StorageObject {
    /// Loads a new instance of `Storage`.
    fn load(environment: &Environment) -> Result<Self, CoordinatorError>
    where
        Self: Sized;

    /// Initializes the location corresponding to the given locator with the given size.
    fn initialize(&mut self, locator: Locator, size: u64) -> Result<(), CoordinatorError>;

    /// Returns `true` if a given locator exists in storage. Otherwise, returns `false`.
    fn exists(&self, locator: &Locator) -> bool;

    /// Returns a copy of an object at the given locator in storage, if it exists.
    fn get(&self, locator: &Locator) -> Result<Object, CoordinatorError>;

    /// Inserts a new object at the given locator into storage, if it does not exist.
    fn insert(&mut self, locator: Locator, object: Object) -> Result<(), CoordinatorError>;

    /// Updates an existing object for the given locator in storage, if it exists.
    fn update(&mut self, locator: &Locator, object: Object) -> Result<(), CoordinatorError>;

    /// Copies the object in the given source locator to the given destination locator.
    fn copy(&mut self, source_locator: &Locator, destination_locator: &Locator) -> Result<(), CoordinatorError>;

    /// Returns the size of the object stored at the given locator.
    fn size(&self, locator: &Locator) -> Result<u64, CoordinatorError>;

    /// Removes a object from storage for a given locator.
    fn remove(&mut self, locator: &Locator) -> Result<(), CoordinatorError>;
}

pub trait StorageObject {
    /// Returns an object reader for the given locator.
    fn reader(&self, locator: &Locator) -> Result<ObjectReader, CoordinatorError>;
    // fn reader<'a>(&self, locator: &Locator) -> Result<dyn ObjectReader<'a>, CoordinatorError>
    // where
    //     dyn ObjectReader<'a>: Sized;

    /// Returns an object writer for the given locator.
    fn writer(&self, locator: &Locator) -> Result<ObjectWriter, CoordinatorError>;
}

pub trait StorageLocator {
    /// Returns a locator path string corresponding to the given locator.
    fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError>;

    /// Returns a locator corresponding to the given locator path string.
    fn to_locator(&self, path: &String) -> Result<Locator, CoordinatorError>;
}

// /// Returns the object for a given locator from storage, if it exists.
// fn get(&self, locator: &Locator) -> Option<Object>;

// pub trait CeremonyData {
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

// /// Returns the contribution locator for a given round, chunk ID, and
// /// contribution ID from the coordinator.
// fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String;

// /// Initializes the contribution locator file for a given round, chunk ID, and
// /// contribution ID from the coordinator.
// fn contribution_locator_init(&mut self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool);

// /// Returns `true` if the contribution locator for a given round height, chunk ID,
// /// and contribution ID exists. Otherwise, returns `false`.
// fn contribution_locator_exists(
//     &self,
//     round_height: u64,
//     chunk_id: u64,
//     contribution_id: u64,
//     verified: bool,
// ) -> bool;

// /// Returns the round locator for a given round from the coordinator.
// fn round_locator(&self, round_height: u64) -> String;

// /// Returns `true` if the round locator for a given round height exists.
// /// Otherwise, returns `false`.
// fn round_locator_exists(&self, round_height: u64) -> bool;
// }
