// use crate::{
//     environment::Environment,
//     storage::{InMemory, Key, Locator, Storage, Value},
//     CoordinatorError,
// };
//
// // use fs2::FileExt;
// use std::{
//     fs::{File, OpenOptions},
//     io::BufReader,
//     path::Path,
//     sync::{Arc, RwLock},
// };
// use tracing::warn;
//
// #[derive(Debug)]
// pub struct Disk {
//     file: File,
//     in_memory: Arc<RwLock<InMemory>>,
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
//                 serde_json::to_writer_pretty(file, &InMemory::load(environment)?)?;
//             }
//         }
//
//         // Open the file in read-only mode and read the JSON contents
//         // of the file as an instance of `InMemory`.
//         let in_memory: InMemory = serde_json::from_reader(BufReader::new(
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
//             file,
//             in_memory: Arc::new(RwLock::new(in_memory)),
//             base_directory: base_directory.to_string(),
//         })
//     }
//
//     /// Returns the value reference for a given key from storage, if it exists.
//     #[inline]
//     fn get(&self, key: &Key) -> Option<Value> {
//         match self.in_memory.read() {
//             Ok(in_memory) => in_memory.get(key).clone(),
//             _ => None,
//         }
//     }
//
//     /// Returns `true` if a given key exists in storage. Otherwise, returns `false`.
//     #[inline]
//     fn contains_key(&self, key: &Key) -> bool {
//         match self.in_memory.read() {
//             Ok(in_memory) => in_memory.contains_key(key),
//             _ => false,
//         }
//     }
//
//     /// Inserts a new key value pair into storage,
//     /// updating the current value for a given key if it exists.
//     /// If successful, returns `true`. Otherwise, returns `false`.
//     #[inline]
//     fn insert(&mut self, key: Key, value: Value) -> bool {
//         match self.in_memory.write() {
//             Ok(mut in_memory) => in_memory.insert(key, value),
//             _ => false,
//         }
//     }
//
//     /// Removes a value from storage for a given key.
//     /// If successful, returns `true`. Otherwise, returns `false`.
//     #[inline]
//     fn remove(&mut self, key: &Key) -> bool {
//         match self.in_memory.write() {
//             Ok(mut in_memory) => in_memory.remove(key),
//             _ => false,
//         }
//     }
// }
//
// impl Locator for Disk {
//     /// Returns the round directory for a given round height from the coordinator.
//     fn round_directory(&self, round_height: u64) -> String
//     where
//         Self: Sized,
//     {
//         format!("{}/round_{}", self.base_directory, round_height)
//     }
//
//     /// Initializes the round directory and round height.
//     fn round_directory_init(&self, round_height: u64) {
//         // If the path does not exist, attempt to initialize the directory path.
//         let path = self.round_directory(round_height);
//
//         if !Path::new(&path).exists() {
//             std::fs::create_dir_all(&path).expect("unable to create the chunk directory");
//         }
//     }
//
//     /// Returns `true` if the round directory for a given round height exists.
//     /// Otherwise, returns `false`.
//     fn round_directory_exists(&self, round_height: u64) -> bool
//     where
//         Self: Sized,
//     {
//         let path = self.round_directory(round_height);
//         Path::new(&path).exists()
//     }
//
//     /// Resets the round directory and round height.
//     fn round_directory_reset(&self, environment: &Environment, round_height: u64) {
//         // If this is a test  attempt to clear it for the coordinator.
//         let directory = self.round_directory(round_height);
//         let path = Path::new(&directory);
//         match environment {
//             Environment::Test(_) => {
//                 if path.exists() {
//                     warn!("Coordinator is clearing {:?}", &path);
//                     std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//                     warn!("Coordinator cleared {:?}", &path);
//                 }
//             }
//             Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//             Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//         }
//     }
//
//     /// Resets the entire round directory.
//     fn round_directory_reset_all(&self, environment: &Environment) {
//         // If this is a test attempt to clear it for the coordinator.
//         let path = Path::new(&self.base_directory);
//         match environment {
//             Environment::Test(_) => {
//                 if path.exists() {
//                     warn!("Coordinator is clearing {:?}", &path);
//                     std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//                     warn!("Coordinator cleared {:?}", &path);
//                 }
//             }
//             Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//             Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//         }
//     }
//
//     /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
//     fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String
//     where
//         Self: Sized,
//     {
//         // Create the transcript directory path.
//         let path = self.round_directory(round_height);
//
//         // Create the chunk directory as `{round_directory}/chunk_{chunk_id}`.
//         format!("{}/chunk_{}", path, chunk_id)
//     }
//
//     /// Initializes the chunk directory for a given  round height, and chunk ID.
//     fn chunk_directory_init(&self, round_height: u64, chunk_id: u64) {
//         // If the path does not exist, attempt to initialize the directory path.
//         let path = self.chunk_directory(round_height, chunk_id);
//
//         if !Path::new(&path).exists() {
//             std::fs::create_dir_all(&path).expect("unable to create the chunk directory");
//         }
//     }
//
//     /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
//     /// Otherwise, returns `false`.
//     fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool
//     where
//         Self: Sized,
//     {
//         let path = self.chunk_directory(round_height, chunk_id);
//         Path::new(&path).exists()
//     }
//
//     /// Returns the contribution locator for a given round, chunk ID, and
//     /// contribution ID from the coordinator.
//     fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String
//     where
//         Self: Sized,
//     {
//         // Fetch the chunk directory path.
//         let path = self.chunk_directory(round_height, chunk_id);
//
//         let verified_str = if verified { "_verified" } else { "" };
//         // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}{verified_str}`.
//         format!("{}/contribution_{}{}", path, contribution_id, verified_str)
//     }
//
//     /// Initializes the contribution locator file for a given round, chunk ID, and
//     /// contribution ID from the coordinator.
//     fn contribution_locator_init(&self, round_height: u64, chunk_id: u64, contribution_id: u64) {
//         // If the path does not exist, attempt to initialize the file path.
//         let path = self.contribution_locator(round_height, chunk_id, contribution_id, false);
//
//         let directory = Path::new(&path).parent().expect("unable to create parent directory");
//         if !directory.exists() {
//             std::fs::create_dir_all(&path).expect("unable to create the contribution directory");
//         }
//     }
//
//     /// Returns `true` if the contribution locator for a given round height, chunk ID,
//     /// and contribution ID exists. Otherwise, returns `false`.
//     fn contribution_locator_exists(
//         &self,
//         round_height: u64,
//         chunk_id: u64,
//         contribution_id: u64,
//         verified: bool,
//     ) -> bool
//     where
//         Self: Sized,
//     {
//         let path = self.contribution_locator(round_height, chunk_id, contribution_id, verified);
//         Path::new(&path).exists()
//     }
//
//     /// Returns the round locator for a given round from the coordinator.
//     fn round_locator(&self, round_height: u64) -> String
//     where
//         Self: Sized,
//     {
//         // Create the transcript directory path.
//         let path = self.round_directory(round_height);
//
//         // Create the round locator located at `{round_directory}/output`.
//         format!("{}/output", path)
//     }
//
//     /// Returns `true` if the round locator for a given round height exists.
//     /// Otherwise, returns `false`.
//     fn round_locator_exists(&self, round_height: u64) -> bool
//     where
//         Self: Sized,
//     {
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
