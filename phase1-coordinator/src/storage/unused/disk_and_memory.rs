// use crate::{
//     environment::Environment,
//     storage::{
//         CeremonyData,
//         Locator,
//         Memory,
//         Object,
//         ObjectReader,
//         ObjectWriter,
//         Storage,
//         StorageLocator,
//         StorageObject,
//     },
//     CoordinatorError,
// };
// use phase1::helpers::CurveKind;
//
// // use fs2::FileExt;
// use itertools::Itertools;
// use memmap::{Mmap, MmapOptions};
// use serde::{
//     de::{self, Deserializer},
//     ser::{self, Serializer},
//     Deserialize,
//     Serialize,
// };
// use std::{
//     fs::{self, File, OpenOptions},
//     io::{BufReader, BufWriter, Read, Write},
//     path::Path,
//     str::FromStr,
//     sync::{Arc, RwLock},
// };
// use tracing::{debug, error, trace, warn};
// use zexe_algebra::{Bls12_377, BW6_761};
//
// #[derive(Debug)]
// pub struct Disk {
//     environment: Environment,
//     file: File,
//     in_memory: Arc<RwLock<Memory>>,
//     base_directory: String,
// }
//
// impl Storage for Disk {
//     /// Loads a new instance of `Disk`.
//     #[inline]
//     fn load(environment: &Environment) -> Result<Self, CoordinatorError>
//     where
//         Self: Sized,
//     {
//         // Fetch the storage base directory.
//         let base_directory = environment.local_base_directory();
//         // Fetch the storage file path.
//         let storage_file = format!("{}/storage.json", base_directory);
//         {
//             // Check the base directory exists.
//             if !Path::new(base_directory).exists() {
//                 // Create the base directory if it does not exist.
//                 std::fs::create_dir_all(base_directory).expect("unable to create the base directory");
//             }
//             // Check that the storage file exists. If not, create a new storage file.
//             if !Path::new(&storage_file).exists() {
//                 // Create and store a new instance of `InMemory`.
//                 let file = OpenOptions::new()
//                     .read(true)
//                     .write(true)
//                     .create(true)
//                     .open(&storage_file)
//                     .expect("unable to open the storage file");
//                 serde_json::to_writer_pretty(file, &Memory::load(environment)?)?;
//             }
//         }
//
//         // Open the file in read-only mode and read the JSON contents
//         // of the file as an instance of `InMemory`.
//         let in_memory: Memory = serde_json::from_reader(BufReader::new(
//             OpenOptions::new()
//                 .read(true)
//                 .open(&storage_file)
//                 .expect("unable to open the storage file"),
//         ))?;
//
//         // Create a writer.
//         let file = OpenOptions::new()
//             .read(true)
//             .write(true)
//             .open(&storage_file)
//             .expect("unable to open the storage file");
//
//         Ok(Self {
//             environment: environment.clone(),
//             file,
//             in_memory: Arc::new(RwLock::new(in_memory)),
//             base_directory: base_directory.to_string(),
//         })
//     }
//
//     /// Initializes the location corresponding to the given locator.
//     /// If successful, returns `true`. Otherwise, returns `false`.
//     fn initialize(&mut self, locator: &Locator) -> Result<(), CoordinatorError> {
//         // Check that the locator does not exist in storage.
//         if self.exists(&locator) {
//             return Err(CoordinatorError::StorageLocatorAlreadyExists);
//         }
//
//         match locator {
//             Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => {
//                 // If the path does not exist, attempt to initialize the file path.
//                 self.chunk_directory_init(*round_height, *chunk_id);
//
//                 let path = self.contribution_locator(*round_height, *chunk_id, *contribution_id, false);
//                 // let directory = Path::new(&path).parent()?;
//                 Ok(std::fs::create_dir_all(&path)?)
//             }
//             _ => return Err(CoordinatorError::StorageFailed),
//         }
//     }
//
//     /// Returns `true` if a given locator exists in storage. Otherwise, returns `false`.
//     #[inline]
//     fn exists(&self, locator: &Locator) -> bool {
//         match self.in_memory.read() {
//             Ok(in_memory) => in_memory.exists(locator),
//             _ => false,
//         }
//     }
//
//     /// Returns the size of the object stored at the given locator.
//     #[inline]
//     fn size(&self, locator: &Locator) -> Result<u64, CoordinatorError> {
//         let reader = OpenOptions::new().read(true).open(self.to_path(locator))?;
//         Ok(reader.metadata()?.len())
//     }
//
//     /// Returns the object for a given locator from storage, if it exists.
//     #[inline]
//     fn get(&self, locator: &Locator) -> Option<Object> {
//         match self.in_memory.read() {
//             Ok(in_memory) => in_memory.get(locator).clone(),
//             _ => None,
//         }
//     }
//
//     /// Inserts a new object at the locator into storage,
//     /// updating the current object for a given locator if it exists.
//     /// If successful, returns `true`. Otherwise, returns `false`.
//     #[inline]
//     fn insert(&mut self, locator: Locator, object: Object) -> bool {
//         match self.in_memory.write() {
//             Ok(mut in_memory) => {
//                 if in_memory.insert(locator, object) {
//                     // Create a writer.
//                     // let file = OpenOptions::new()
//                     //     .read(true)
//                     //     .write(true)
//                     //     .open(&locator.to_string())
//                     //     .expect("unable to open the storage file");
//                     return true;
//                 }
//                 false
//             }
//             _ => false,
//         }
//     }
//
//     /// Copies the object in the given source locator to the given destination locator.
//     #[inline]
//     fn copy(&mut self, source_locator: &Locator, destination_locator: &Locator) -> Result<(), CoordinatorError> {
//         // Check that the given source locator exists in storage.
//         if !self.exists(&source_locator) {
//             return Err(CoordinatorError::StorageLocatorMissing);
//         }
//
//         // Check that the given source locator exists in storage.
//         if !self.exists(&destination_locator) {
//             return Err(CoordinatorError::StorageLocatorMissing);
//         }
//
//         let mut in_memory = self.in_memory.write()?;
//
//         if let Ok(_) = in_memory.copy(source_locator, destination_locator) {
//             trace!("Copying {} to {}", source_locator, destination_locator);
//             fs::copy(&self.to_path(source_locator)?, &self.to_path(destination_locator)?)?;
//             trace!("Copied {} to {}", source_locator, destination_locator);
//             return Ok(());
//         }
//
//         Err(CoordinatorError::StorageCopyFailed)
//     }
//
//     /// Removes an object from storage for a given locator.
//     /// If successful, returns `true`. Otherwise, returns `false`.
//     #[inline]
//     fn remove(&mut self, locator: &Locator) -> bool {
//         match self.in_memory.write() {
//             Ok(mut in_memory) => in_memory.remove(locator),
//             _ => false,
//         }
//     }
// }
//
// impl StorageLocator for Disk {
//     #[inline]
//     fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError> {
//         Ok(match locator {
//             Locator::RoundHeight => "rh://".to_string(),
//             Locator::Round(round_height) => format!("r://{}", round_height),
//             Locator::RoundFile(round_height) => format!("rf://{}", round_height),
//             Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => format!(
//                 "cf://{}.{}.{}.{}",
//                 round_height, chunk_id, contribution_id, *verified as u64
//             ),
//             _ => return Err(CoordinatorError::LocatorSerializationFailed),
//         })
//         // Ok(serde_json::to_string(locator)?)
//     }
//
//     #[inline]
//     fn to_locator(&self, path: &String) -> Result<Locator, CoordinatorError> {
//         let (variant, data) = match path.splitn(2, "://").collect_tuple() {
//             Some((variant, data)) => (variant, data),
//             None => return Err(CoordinatorError::LocatorDeserializationFailed),
//         };
//         match (variant, data) {
//             ("rh", "") => Ok(Locator::RoundHeight),
//             ("r", value) => Ok(Locator::Round(u64::from_str(value)?)),
//             ("rf", value) => Ok(Locator::RoundFile(u64::from_str(value)?)),
//             ("cf", value) => match value.splitn(4, ".").map(u64::from_str).collect_tuple() {
//                 Some((round_height, chunk_id, contribution_id, verified)) => Ok(Locator::ContributionFile(
//                     round_height?,
//                     chunk_id?,
//                     contribution_id?,
//                     verified? != 0,
//                 )),
//                 None => Err(CoordinatorError::LocatorDeserializationFailed),
//             },
//             _ => Err(CoordinatorError::LocatorDeserializationFailed),
//         }
//         // Ok(serde_json::from_str(locator)?)
//     }
// }
//
// impl StorageObject for Disk {
//     /// Returns an object reader for the given locator.
//     fn reader(&self, locator: &Locator) -> Result<ObjectReader, CoordinatorError> {
//         // Check that the locator exists in storage.
//         if !self.exists(&locator) {
//             return Err(CoordinatorError::StorageLocatorMissing);
//         }
//
//         match locator {
//             Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => {
//                 let reader = OpenOptions::new().read(true).open(self.to_path(locator)?)?;
//
//                 // Derive the expected file size of the contribution.
//                 let compressed = self.environment.compressed_outputs();
//                 let settings = self.environment.to_settings();
//                 let (_, _, curve, _, _, _) = settings;
//                 let expected = match curve {
//                     CurveKind::Bls12_377 => contribution_filesize!(Bls12_377, settings, chunk_id, compressed),
//                     CurveKind::BW6 => contribution_filesize!(BW6_761, settings, chunk_id, compressed),
//                 };
//
//                 // Check that contribution filesize is correct.
//                 let found = self.size(&locator)?;
//                 debug!("Round {} chunk {} filesize is {}", round_height, chunk_id, found);
//                 if found != expected {
//                     error!("Contribution file size should be {} but found {}", expected, found);
//                     return Err(CoordinatorError::ContributionFileSizeMismatch.into());
//                 }
//
//                 Ok(unsafe { MmapOptions::new().map(&reader)? })
//             }
//             _ => Err(CoordinatorError::StorageFailed),
//         }
//     }
//
//     /// Returns an object writer for the given locator.
//     fn writer(&self, locator: &Locator) -> Result<ObjectWriter, CoordinatorError> {
//         // Check that the locator exists in storage.
//         if !self.exists(&locator) {
//             return Err(CoordinatorError::StorageLocatorMissing);
//         }
//
//         match locator {
//             Locator::RoundFile(round_height) => {
//                 let writer = OpenOptions::new()
//                     .read(true)
//                     .write(true)
//                     .create_new(true)
//                     .open(locator)?;
//
//                 // Check the round filesize will fit on the system.
//                 let is_initial = round_height == 0;
//                 let compressed = self.environment.compressed_inputs();
//                 let settings = self.environment.to_settings();
//                 let (_, _, curve, _, _, _) = settings;
//                 let round_size = match curve {
//                     CurveKind::Bls12_377 => round_filesize!(Bls12_377, settings, chunk_id, compressed, is_initial),
//                     CurveKind::BW6 => round_filesize!(BW6_761, settings, chunk_id, compressed, is_initial),
//                 };
//                 debug!("Round {} filesize will be {}", round_height, round_size);
//                 writer.set_len(round_size).expect("round file must be large enough");
//
//                 Ok(unsafe { MmapOptions::new().map_mut(&writer)? })
//             }
//             _ => Err(CoordinatorError::StorageFailed),
//         }
//     }
// }
//
// // impl Serialize for Locator {
// //     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// //     where
// //         S: Serializer,
// //     {
// //         serializer.serialize_str(&match self {
// //             Locator::RoundHeight => "rh://".to_string(),
// //             Locator::Round(round_height) => format!("r://{}", round_height),
// //             Locator::RoundFile(round_height) => format!("rf://{}", round_height),
// //             Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => format!(
// //                 "cf://{}.{}.{}.{}",
// //                 round_height, chunk_id, contribution_id, *verified as u64
// //             ),
// //             // Locator::Ping => "ping://".to_string(),
// //             _ => return Err(ser::Error::custom("invalid serialization key")),
// //         })
// //     }
// // }
//
// // impl<'de> Deserialize<'de> for Locator {
// //     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
// //     where
// //         D: Deserializer<'de>,
// //     {
// //         let s = String::deserialize(deserializer)?;
// //         let (variant, data) = match s.splitn(2, "://").collect_tuple() {
// //             Some((variant, data)) => (variant, data),
// //             None => return Err(de::Error::custom("failed to parse serialization key")),
// //         };
// //         match (variant, data) {
// //             ("rh", "") => Ok(Locator::RoundHeight),
// //             ("r", value) => Ok(Locator::Round(u64::from_str(value).map_err(de::Error::custom)?)),
// //             ("rf", value) => Ok(Locator::RoundFile(u64::from_str(value).map_err(de::Error::custom)?)),
// //             ("cf", value) => match s.splitn(4, ".").map(u64::from_str).collect_tuple() {
// //                 Some((round_height, chunk_id, contribution_id, verified)) => Ok(Locator::ContributionFile(
// //                     round_height.map_err(de::Error::custom)?,
// //                     chunk_id.map_err(de::Error::custom)?,
// //                     contribution_id.map_err(de::Error::custom)?,
// //                     verified.map_err(de::Error::custom)? as bool,
// //                 )),
// //                 None => Err(de::Error::custom("failed to parse serialization key")),
// //             },
// //             ("ping", "") => Ok(Locator::Ping),
// //             _ => Err(de::Error::custom("invalid deserialization key")),
// //         }
// //     }
// // }
//
// impl Disk {
//     /// Returns the round directory for a given round height from the coordinator.
//     fn round_directory(&self, round_height: u64) -> String {
//         format!("{}/round_{}", self.base_directory, round_height)
//     }
//
//     /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
//     fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
//         // Fetch the transcript directory path.
//         let path = self.round_directory(round_height);
//
//         // Format the chunk directory as `{round_directory}/chunk_{chunk_id}`.
//         format!("{}/chunk_{}", path, chunk_id)
//     }
//
//     /// Initializes the chunk directory for a given  round height, and chunk ID.
//     fn chunk_directory_init(&mut self, round_height: u64, chunk_id: u64) {
//         // If the round directory does not exist, attempt to initialize the directory path.
//         let path = self.round_directory(round_height);
//         if !Path::new(&path).exists() {
//             std::fs::create_dir_all(&path).expect("unable to create the round directory");
//         }
//
//         // If the chunk directory does not exist, attempt to initialize the directory path.
//         let path = self.chunk_directory(round_height, chunk_id);
//         if !Path::new(&path).exists() {
//             std::fs::create_dir_all(&path).expect("unable to create the chunk directory");
//         }
//     }
//
//     /// Returns the round locator for a given round from the coordinator.
//     fn round_locator(&self, round_height: u64) -> String {
//         // Fetch the transcript directory path.
//         let path = self.round_directory(round_height);
//
//         // Format the round locator located at `{round_directory}/output`.
//         format!("{}/output", path)
//     }
// }
//
// impl CeremonyData for Disk {
//     // /// Initializes the round directory for a given round height.
//     // fn round_directory_init(&mut self, round_height: u64) {
//     //     // If the path does not exist, attempt to initialize the directory path.
//     //     let path = self.round_directory(round_height);
//     //     if !Path::new(&path).exists() {
//     //         std::fs::create_dir_all(&path).expect("unable to create the round directory");
//     //     }
//     // }
//
//     // /// Returns `true` if the round directory for a given round height exists.
//     // /// Otherwise, returns `false`.
//     // fn round_directory_exists(&self, round_height: u64) -> bool {
//     //     let path = self.round_directory(round_height);
//     //     Path::new(&path).exists()
//     // }
//
//     // /// Resets the round directory for a given round height.
//     // fn round_directory_reset(&mut self, environment: &Environment, round_height: u64) {
//     //     // If this is a test  attempt to clear it for the coordinator.
//     //     let directory = self.round_directory(round_height);
//     //     let path = Path::new(&directory);
//     //     match environment {
//     //         Environment::Test(_) => {
//     //             if path.exists() {
//     //                 warn!("Coordinator is clearing {:?}", &path);
//     //                 std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//     //                 warn!("Coordinator cleared {:?}", &path);
//     //             }
//     //         }
//     //         Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//     //         Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//     //     }
//     // }
//
//     // /// Resets the entire round directory.
//     // fn round_directory_reset_all(&mut self, environment: &Environment) {
//     //     // If this is a test attempt to clear it for the coordinator.
//     //     let path = Path::new(&self.base_directory);
//     //     match environment {
//     //         Environment::Test(_) => {
//     //             if path.exists() {
//     //                 warn!("Coordinator is clearing {:?}", &path);
//     //                 std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//     //                 warn!("Coordinator cleared {:?}", &path);
//     //             }
//     //         }
//     //         Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//     //         Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//     //     }
//     // }
//
//     // /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
//     // /// Otherwise, returns `false`.
//     // fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool {
//     //     let path = self.chunk_directory(round_height, chunk_id);
//     //     Path::new(&path).exists()
//     // }
//
//     /// Returns the contribution locator for a given round, chunk ID, and
//     /// contribution ID from the coordinator.
//     fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String {
//         // Fetch the chunk directory path.
//         let path = self.chunk_directory(round_height, chunk_id);
//
//         // As the contribution at ID 0 is a continuation of the last contribution
//         // in the previous round, it will always be verified by default.
//         match verified || contribution_id == 0 {
//             // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.verified`.
//             true => format!("{}/contribution_{}.verified", path, contribution_id),
//             // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.unverified`.
//             false => format!("{}/contribution_{}.unverified", path, contribution_id),
//         }
//     }
//
//     // /// Initializes the contribution locator file for a given round, chunk ID, and
//     // /// contribution ID from the coordinator.
//     // fn contribution_locator_init(&mut self, round_height: u64, chunk_id: u64, contribution_id: u64, _verified: bool) {
//     //     // If the path does not exist, attempt to initialize the file path.
//     //     self.chunk_directory_init(round_height, chunk_id);
//     //
//     //     let path = self.contribution_locator(round_height, chunk_id, contribution_id, false);
//     //     let directory = Path::new(&path).parent().expect("unable to create parent directory");
//     //     if !directory.exists() {
//     //         std::fs::create_dir_all(&path).expect("unable to create the contribution directory");
//     //     }
//     // }
//
//     // /// Returns `true` if the contribution locator for a given round height, chunk ID,
//     // /// and contribution ID exists. Otherwise, returns `false`.
//     // fn contribution_locator_exists(
//     //     &self,
//     //     round_height: u64,
//     //     chunk_id: u64,
//     //     contribution_id: u64,
//     //     verified: bool,
//     // ) -> bool {
//     //     let path = self.contribution_locator(round_height, chunk_id, contribution_id, verified);
//     //     Path::new(&path).exists()
//     // }
//
//     /// Returns `true` if the round locator for a given round height exists.
//     /// Otherwise, returns `false`.
//     fn round_locator_exists(&self, round_height: u64) -> bool {
//         let path = self.round_locator(round_height);
//         Path::new(&path).exists()
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::{
//         storage::{Disk, Storage},
//         testing::prelude::*,
//     };
//
//     use std::{collections::HashMap, fs::OpenOptions, io::BufReader, path::Path};
//
//     fn load_test() -> anyhow::Result<()> {
//         let environment = &TEST_ENVIRONMENT_3;
//
//         // Create a new instance.
//         let _storage = Disk::load(&environment)?;
//
//         // Check the base directory exists.
//         let base_directory = environment.local_base_directory();
//         assert!(Path::new(base_directory).exists());
//
//         // Check the storage file path exists.
//         let storage_file = format!("{}/storage.json", base_directory);
//         assert!(Path::new(&storage_file).exists());
//
//         // Open the file in read-only mode with buffer.
//         let reader = BufReader::new(
//             OpenOptions::new()
//                 .read(true)
//                 .open(&storage_file)
//                 .expect("unable to open the storage file"),
//         );
//
//         // Read the JSON contents of the file.
//         let in_memory: HashMap<String, String> = serde_json::from_reader(reader)?;
//
//         // Check that the storage key exists.
//         assert!(in_memory.contains_key("storage"));
//
//         Ok(())
//     }
//
//     #[test]
//     #[serial]
//     #[ignore]
//     fn test_load() {
//         clear_test_transcript();
//         load_test().unwrap();
//     }
// }
