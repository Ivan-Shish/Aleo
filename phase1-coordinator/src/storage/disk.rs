use crate::{
    environment::Environment,
    objects::Round,
    storage::{Locator, Object, ObjectReader, ObjectWriter, Storage, StorageLocator, StorageObject},
    CoordinatorError,
};

use itertools::Itertools;
use memmap::{MmapMut, MmapOptions};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub struct Disk {
    environment: Environment,
    manifest: Arc<RwLock<DiskManifest>>,
    locators: HashMap<Locator, (Arc<RwLock<MmapMut>>, File)>,
    locator_path: DiskLocator,
}

impl Storage for Disk {
    /// Loads a new instance of `Disk`.
    #[inline]
    fn load(environment: &Environment) -> Result<Self, CoordinatorError>
    where
        Self: Sized,
    {
        trace!("Loading storage");

        // Load the manifest for storage from disk.
        let manifest = DiskManifest::load(environment.local_base_directory())?;
        let locators_to_load = manifest.locators.clone();

        // Create the `Storage` instance.
        let mut storage = Self {
            environment: environment.clone(),
            manifest: Arc::new(RwLock::new(manifest)),
            locators: HashMap::default(),
            locator_path: DiskLocator {
                base: environment.local_base_directory().to_string(),
            },
        };

        // Load the locators from the manifest.
        for locator in locators_to_load.iter() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&storage.to_path(locator)?)?;
            storage.locators.insert(
                locator.clone(),
                (
                    Arc::new(RwLock::new(unsafe { MmapOptions::new().map_mut(&file)? })),
                    file,
                ),
            );
        }

        // Create the round height locator if it does not exist yet.
        if !storage.exists(&Locator::RoundHeight) {
            storage.insert(Locator::RoundHeight, Object::RoundHeight(0))?;
        }

        trace!("Loaded storage");
        Ok(storage)
    }

    /// Initializes the location corresponding to the given locator.
    fn initialize(&mut self, locator: Locator, size: u64) -> Result<(), CoordinatorError> {
        trace!("Initializing {}", self.to_path(&locator)?);

        // Check that the locator does not exist in storage.
        if self.exists(&locator) {
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Acquire the manifest file write lock.
        let mut manifest = self.manifest.write().unwrap();

        // Initialize the directory for contribution files, if it does not exist.
        if let Locator::ContributionFile(round_height, chunk_id, _, _) = locator {
            // If the file directory does not exist, attempt to initialize it.
            self.locator_path.chunk_directory_init(round_height, chunk_id);
        }

        // Load the file path.
        let path = self.to_path(&locator)?;

        // Create the new file.
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)?;

        // Set the file to the given size.
        file.set_len(size)?;

        // Add the file to the locators.
        self.locators.insert(
            locator.clone(),
            (
                Arc::new(RwLock::new(unsafe { MmapOptions::new().map_mut(&file)? })),
                file,
            ),
        );

        // Add the locator to the manifest.
        if !manifest.locators.insert(locator.clone()) {
            error!("{} already exists", self.to_path(&locator)?);
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Save the manifest update to disk.
        manifest.save()?;

        trace!("Initialized {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Returns `true` if a given locator exists in storage. Otherwise, returns `false`.
    #[inline]
    fn exists(&self, locator: &Locator) -> bool {
        self.manifest.read().unwrap().contains(locator) && self.locators.contains_key(locator)
    }

    /// Returns a copy of an object at the given locator in storage, if it exists.
    fn get(&self, locator: &Locator) -> Result<Object, CoordinatorError> {
        trace!("Fetching {}", self.to_path(locator)?);

        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            trace!("Locator missing in call to get() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Acquire the file read lock.
        let reader = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .0
            .read()
            .unwrap();

        let object = match locator {
            Locator::RoundHeight => {
                let round_height: u64 = serde_json::from_slice(&*reader)?;
                Ok(Object::RoundHeight(round_height))
            }
            Locator::RoundState(_) => {
                let round: Round = serde_json::from_slice(&*reader)?;
                Ok(Object::RoundState(round))
            }
            Locator::RoundFile(round_height) => {
                // Check that the round size is correct.
                let expected = Object::round_file_size(&self.environment, *round_height);
                let found = self.size(&locator)?;
                debug!("Round {} filesize is {}", round_height, found);
                if found == 0 || expected != found {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }

                let mut round_file: Vec<u8> = Vec::with_capacity(expected as usize);
                round_file.write_all(&*reader)?;
                Ok(Object::RoundFile(round_file))
            }
            Locator::ContributionFile(round_height, chunk_id, _, verified) => {
                // Check that the contribution size is correct.
                let expected = Object::contribution_file_size(&self.environment, *chunk_id, *verified);
                let found = self.size(&locator)?;
                debug!("Round {} chunk {} filesize is {}", round_height, chunk_id, found);
                if found == 0 || expected != found {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::ContributionFileSizeMismatch.into());
                }

                let mut contribution_file: Vec<u8> = Vec::with_capacity(expected as usize);
                contribution_file.write_all(&*reader)?;
                Ok(Object::ContributionFile(contribution_file))
            }
        };

        trace!("Fetched {}", self.to_path(locator)?);
        object
    }

    /// Inserts a new object at the given locator into storage, if it does not exist.
    #[inline]
    fn insert(&mut self, locator: Locator, object: Object) -> Result<(), CoordinatorError> {
        trace!("Inserting {}", self.to_path(&locator)?);

        // Check that the given locator does not exist in storage.
        if self.exists(&locator) {
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Initialize the new file with the object size.
        self.initialize(locator.clone(), object.size())?;

        // Insert the object at the given locator.
        self.update(&locator, object)?;

        trace!("Inserted {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Updates an existing object for the given locator in storage, if it exists.
    #[inline]
    fn update(&mut self, locator: &Locator, object: Object) -> Result<(), CoordinatorError> {
        trace!("Updating {}", self.to_path(locator)?);

        // Check that the given locator exists in storage.
        if !self.exists(locator) {
            trace!("Locator missing in call to update() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Acquire the file write lock.
        let mut writer = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .0
            .write()
            .unwrap();

        // Set the file size to the size of the given object.
        let file = self.locators.get(locator).ok_or(CoordinatorError::StorageLockFailed)?;
        file.1.set_len(object.size())?;

        // Update the writer.
        *writer = unsafe { MmapOptions::new().map_mut(&file.1)? };

        // Write the new object to the file.
        (*writer).as_mut().write_all(&object.to_bytes())?;

        // Sync all in-memory data to disk.
        writer.flush()?;

        trace!("Updated {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Copies an object from the given source locator to the given destination locator.
    #[inline]
    fn copy(&mut self, source_locator: &Locator, destination_locator: &Locator) -> Result<(), CoordinatorError> {
        trace!(
            "Copying from A to B\n\n\tA: {}\n\tB: {}\n",
            self.to_path(source_locator)?,
            self.to_path(destination_locator)?
        );

        // Check that the given source locator exists in storage.
        if !self.exists(source_locator) {
            trace!("Locator missing in call to copy() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Check that the given destination locator does NOT exist in storage.
        if self.exists(destination_locator) {
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Fetch the source object.
        let source_object = self.get(source_locator)?;

        // Initialize the destination file with the source object size.
        self.initialize(destination_locator.clone(), source_object.size())?;

        // Update the destination locator with the copied source object.
        self.update(destination_locator, source_object)?;

        trace!("Copied to {}", self.to_path(destination_locator)?);
        Ok(())
    }

    /// Returns the size of the object stored at the given locator.
    #[inline]
    fn size(&self, locator: &Locator) -> Result<u64, CoordinatorError> {
        trace!("Fetching size of {}", self.to_path(locator)?);

        // Check that the given locator exists in storage.
        if !self.exists(locator) {
            trace!("Locator missing in call to size() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        let size = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .1
            .metadata()?
            .len();

        trace!("Fetched size of {}", self.to_path(&locator)?);
        Ok(size)
    }

    /// Removes the object corresponding to the given locator from storage.
    #[inline]
    fn remove(&mut self, locator: &Locator) -> Result<(), CoordinatorError> {
        trace!("Removing {}", self.to_path(locator)?);

        // Check that the locator does not exist in storage.
        if self.exists(&locator) {
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Acquire the manifest file write lock.
        let mut manifest = self.manifest.write().unwrap();

        // Acquire the file write lock.
        let file = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .0
            .write()
            .unwrap();

        // Fetch the locator file path.
        let path = self.to_path(locator)?;

        trace!("Removing {}", path);
        fs::remove_file(path.clone())?;
        trace!("Removed {}", path);

        // Remove the file write lock.
        drop(file);

        // Remove the locator from the locators.
        self.locators.remove(locator);

        // Remove the locator from the manifest.
        manifest.locators.remove(locator);

        // Save the manifest update to disk.
        manifest.save()?;

        trace!("Removed {}", self.to_path(locator)?);
        Ok(())
    }
}

impl StorageLocator for Disk {
    #[inline]
    fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError> {
        self.locator_path.to_path(locator)
    }

    #[inline]
    fn to_locator(&self, path: &String) -> Result<Locator, CoordinatorError> {
        self.locator_path.to_locator(path)
    }
}

impl StorageObject for Disk {
    /// Returns an object reader for the given locator.
    fn reader<'a>(&self, locator: &Locator) -> Result<ObjectReader, CoordinatorError> {
        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            trace!("Locator missing in call to reader() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Acquire the file read lock.
        let reader = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .0
            .read()
            .unwrap();

        match locator {
            Locator::RoundHeight => Ok(reader),
            Locator::RoundState(_) => Ok(reader),
            Locator::RoundFile(round_height) => {
                // Check that the round size is correct.
                let expected = Object::round_file_size(&self.environment, *round_height);
                let found = self.size(&locator)?;
                debug!("Round {} filesize is {}", round_height, found);
                if found != expected {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }
                Ok(reader)
            }
            Locator::ContributionFile(round_height, chunk_id, _, verified) => {
                // Check that the contribution size is correct.
                let expected = Object::contribution_file_size(&self.environment, *chunk_id, *verified);
                let found = self.size(&locator)?;
                debug!("Round {} chunk {} filesize is {}", round_height, chunk_id, found);
                if found != expected {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::ContributionFileSizeMismatch.into());
                }
                Ok(reader)
            }
        }
    }

    /// Returns an object writer for the given locator.
    fn writer(&self, locator: &Locator) -> Result<ObjectWriter, CoordinatorError> {
        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            trace!("Locator missing in call to writer() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Acquire the file read lock.
        let mut writer = self
            .locators
            .get(locator)
            .ok_or(CoordinatorError::StorageLockFailed)?
            .0
            .write()
            .unwrap();

        match locator {
            Locator::RoundHeight => Ok(writer),
            Locator::RoundState(_) => Ok(writer),
            Locator::RoundFile(round_height) => {
                // Check that the round size is correct.
                let expected = Object::round_file_size(&self.environment, *round_height);
                let found = self.size(&locator)?;
                debug!("Round {} filesize is {}", round_height, found);
                if found != expected {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }
                Ok(writer)
            }
            Locator::ContributionFile(round_height, chunk_id, _, verified) => {
                // Check that the contribution size is correct.
                let expected = Object::contribution_file_size(&self.environment, *chunk_id, *verified);
                let found = self.size(&locator)?;
                debug!("Round {} chunk {} filesize is {}", round_height, chunk_id, found);
                if found != expected {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::ContributionFileSizeMismatch.into());
                }
                Ok(writer)
            }
        }
    }
}

#[derive(Debug)]
struct DiskManifest {
    locators: HashSet<Locator>,
    file: File,
    locator_path: DiskLocator,
}

impl DiskManifest {
    /// Load the manifest for storage from disk.
    #[inline]
    fn load(base_directory: &str) -> Result<Self, CoordinatorError> {
        // Check the base directory exists.
        if !Path::new(base_directory).exists() {
            // Create the base directory if it does not exist.
            std::fs::create_dir_all(base_directory).expect("unable to create the base directory");
        }

        // Create the storage manifest file path.
        let manifest_file = format!("{}/manifest.json", base_directory);

        // Create the locator path.
        let locator_path = DiskLocator {
            base: base_directory.to_string(),
        };

        // Check that the storage file exists. If not, create a new storage file.
        let (locators, file) = match !Path::new(&manifest_file).exists() {
            // Create and store a new instance of `InMemory`.
            true => {
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&manifest_file)?;

                // Set the file length.
                let locators: HashSet<Locator> = HashSet::default();
                let buffer = serde_json::to_vec_pretty(&locators)?;
                file.set_len(buffer.len() as u64)?;

                // Write the buffer to the file.
                file.write_all(&buffer)?;

                (locators, file)
            }
            false => {
                let file = OpenOptions::new().read(true).write(true).open(&manifest_file)?;

                // Convert all paths to locators.
                let paths: HashSet<String> = serde_json::from_reader(BufReader::new(&file))?;
                let locators: HashSet<Locator> = paths
                    .par_iter()
                    .map(|path| locator_path.to_locator(&path).unwrap())
                    .collect();

                (locators, file)
            }
        };

        Ok(Self {
            locators,
            file,
            locator_path,
        })
    }

    #[inline]
    fn save(&mut self) -> Result<(), CoordinatorError> {
        // Convert all locators to paths.
        let paths: HashSet<String> = self
            .locators
            .par_iter()
            .map(|locator| self.locator_path.to_path(&locator).unwrap())
            .collect();

        // Set the file length.
        let buffer = serde_json::to_vec_pretty(&paths)?;
        self.file.set_len(buffer.len() as u64)?;

        // Write the buffer to the file.
        self.file.write_all(&buffer)?;

        Ok(())
    }

    #[inline]
    fn contains(&self, locator: &Locator) -> bool {
        self.locators.contains(locator)
    }
}

#[derive(Debug)]
struct DiskLocator {
    base: String,
}

impl StorageLocator for DiskLocator {
    #[inline]
    fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError> {
        let path = match locator {
            Locator::RoundHeight => format!("{}/round_height", self.base),
            Locator::RoundState(round_height) => format!("{}/state.json", self.round_directory(*round_height)),
            Locator::RoundFile(round_height) => {
                let round_directory = self.round_directory(*round_height);
                format!("{}/round_{}.verified", round_directory, *round_height)
            }
            Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => {
                self.contribution_locator(*round_height, *chunk_id, *contribution_id, *verified)
            }
        };
        // Sanitize the path.
        Ok(Path::new(&path)
            .to_str()
            .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?
            .to_string())
    }

    #[inline]
    fn to_locator(&self, path: &String) -> Result<Locator, CoordinatorError> {
        // Sanitize the given path and base to the local OS.
        let path = Path::new(path);
        let base = Path::new(&self.base);

        // Check that the path matches the expected base.
        if !path.starts_with(base) {
            return Err(CoordinatorError::StorageLocatorFormatIncorrect);
        }

        // Strip the base prefix.
        let key = path
            .strip_prefix(&format!("{}/", self.base))
            .map_err(|_| CoordinatorError::StorageLocatorFormatIncorrect)?;

        let key = key.to_str().ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?;

        // Check if it matches the round height.
        if key == "round_height" {
            return Ok(Locator::RoundHeight);
        }

        // Parse the key into its components.
        if let Some((round, remainder)) = key.splitn(2, "/").collect_tuple() {
            // Check if it resembles the round directory.
            if round.starts_with("round_") {
                // Attempt to parse the round string for the round height.
                let round_height = u64::from_str(
                    round
                        .strip_prefix("round_")
                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?,
                )?;

                // Check if it matches the round directory.
                if round == &format!("round_{}", round_height) {
                    /* In round directory */

                    // Check if it matches the round state.
                    if remainder == "state.json" {
                        return Ok(Locator::RoundState(round_height));
                    }

                    // Check if it matches the round file.
                    if remainder == format!("round_{}.verified", round_height) {
                        return Ok(Locator::RoundState(round_height));
                    }

                    // Parse the path into its components.
                    if let Some((chunk, path)) = remainder.splitn(2, "/").collect_tuple() {
                        // Check if it resembles the chunk directory.
                        if chunk.starts_with("chunk_") {
                            // Attempt to parse the path string for the chunk ID.
                            let chunk_id = u64::from_str(
                                chunk
                                    .strip_prefix("chunk_")
                                    .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?,
                            )?;

                            // Check if it matches the chunk directory.
                            if chunk == &format!("chunk_{}", chunk_id) {
                                let path = Path::new(path);

                                /* In chunk directory */

                                // Check if it matches the contribution file.
                                if path.starts_with("contribution_") {
                                    let (id, extension) = chunk
                                        .strip_prefix("chunk_")
                                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?
                                        .splitn(2, '.')
                                        .collect_tuple()
                                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?;
                                    let contribution_id = u64::from_str(id)?;

                                    // Check if it matches a unverified contribution file.
                                    if extension == "unverified" {
                                        return Ok(Locator::ContributionFile(
                                            round_height,
                                            chunk_id,
                                            contribution_id,
                                            false,
                                        ));
                                    }

                                    // Check if it matches a unverified contribution file.
                                    if extension == "verified" {
                                        return Ok(Locator::ContributionFile(
                                            round_height,
                                            chunk_id,
                                            contribution_id,
                                            true,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(CoordinatorError::StorageLocatorFormatIncorrect)
    }
}

impl DiskLocator {
    /// Returns the round directory for a given round height from the coordinator.
    fn round_directory(&self, round_height: u64) -> String {
        format!("{}/round_{}", self.base, round_height)
    }

    /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        // Fetch the transcript directory path.
        let path = self.round_directory(round_height);

        // Format the chunk directory as `{round_directory}/chunk_{chunk_id}`.
        format!("{}/chunk_{}", path, chunk_id)
    }

    /// Initializes the chunk directory for a given  round height, and chunk ID.
    fn chunk_directory_init(&self, round_height: u64, chunk_id: u64) {
        // If the round directory does not exist, attempt to initialize the directory path.
        let path = self.round_directory(round_height);
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("unable to create the round directory");
        }

        // If the chunk directory does not exist, attempt to initialize the directory path.
        let path = self.chunk_directory(round_height, chunk_id);
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("unable to create the chunk directory");
        }
    }

    /// Returns the contribution locator for a given round, chunk ID, and
    /// contribution ID from the coordinator.
    fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64, verified: bool) -> String {
        // Fetch the chunk directory path.
        let path = self.chunk_directory(round_height, chunk_id);

        // As the contribution at ID 0 is a continuation of the last contribution
        // in the previous round, it will always be verified by default.
        match verified || contribution_id == 0 {
            // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.verified`.
            true => format!("{}/contribution_{}.verified", path, contribution_id),
            // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.unverified`.
            false => format!("{}/contribution_{}.unverified", path, contribution_id),
        }
    }
}

// impl LocatorPath for DiskIndex {
//     #[inline]
//     fn to_path(&self, locator: &Locator) -> Result<String, CoordinatorError> {
//         self.locators.read().unwrap().to_path(locator)
//     }
//
//     #[inline]
//     fn to_locator(&self, path: &String) -> Result<Locator, CoordinatorError> {
//         self.locators.read().unwrap().to_locator(path)
//     }
// }

// impl Serialize for Locator {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&match self {
//             Locator::RoundHeight => "rh://".to_string(),
//             Locator::Round(round_height) => format!("r://{}", round_height),
//             Locator::RoundFile(round_height) => format!("rf://{}", round_height),
//             Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => format!(
//                 "cf://{}.{}.{}.{}",
//                 round_height, chunk_id, contribution_id, *verified as u64
//             ),
//             // Locator::Ping => "ping://".to_string(),
//             _ => return Err(ser::Error::custom("invalid serialization key")),
//         })
//     }
// }

// impl<'de> Deserialize<'de> for Locator {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         let (variant, data) = match s.splitn(2, "://").collect_tuple() {
//             Some((variant, data)) => (variant, data),
//             None => return Err(de::Error::custom("failed to parse serialization key")),
//         };
//         match (variant, data) {
//             ("rh", "") => Ok(Locator::RoundHeight),
//             ("r", value) => Ok(Locator::Round(u64::from_str(value).map_err(de::Error::custom)?)),
//             ("rf", value) => Ok(Locator::RoundFile(u64::from_str(value).map_err(de::Error::custom)?)),
//             ("cf", value) => match s.splitn(4, ".").map(u64::from_str).collect_tuple() {
//                 Some((round_height, chunk_id, contribution_id, verified)) => Ok(Locator::ContributionFile(
//                     round_height.map_err(de::Error::custom)?,
//                     chunk_id.map_err(de::Error::custom)?,
//                     contribution_id.map_err(de::Error::custom)?,
//                     verified.map_err(de::Error::custom)? as bool,
//                 )),
//                 None => Err(de::Error::custom("failed to parse serialization key")),
//             },
//             ("ping", "") => Ok(Locator::Ping),
//             _ => Err(de::Error::custom("invalid deserialization key")),
//         }
//     }
// }

// impl CeremonyData for Disk {
// /// Initializes the round directory for a given round height.
// fn round_directory_init(&mut self, round_height: u64) {
//     // If the path does not exist, attempt to initialize the directory path.
//     let path = self.round_directory(round_height);
//     if !Path::new(&path).exists() {
//         std::fs::create_dir_all(&path).expect("unable to create the round directory");
//     }
// }

// /// Returns `true` if the round directory for a given round height exists.
// /// Otherwise, returns `false`.
// fn round_directory_exists(&self, round_height: u64) -> bool {
//     let path = self.round_directory(round_height);
//     Path::new(&path).exists()
// }

// /// Resets the round directory for a given round height.
// fn round_directory_reset(&mut self, environment: &Environment, round_height: u64) {
//     // If this is a test  attempt to clear it for the coordinator.
//     let directory = self.round_directory(round_height);
//     let path = Path::new(&directory);
//     match environment {
//         Environment::Test(_) => {
//             if path.exists() {
//                 warn!("Coordinator is clearing {:?}", &path);
//                 std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//                 warn!("Coordinator cleared {:?}", &path);
//             }
//         }
//         Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//         Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//     }
// }

// /// Resets the entire round directory.
// fn round_directory_reset_all(&mut self, environment: &Environment) {
//     // If this is a test attempt to clear it for the coordinator.
//     let path = Path::new(&self.base_directory);
//     match environment {
//         Environment::Test(_) => {
//             if path.exists() {
//                 warn!("Coordinator is clearing {:?}", &path);
//                 std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
//                 warn!("Coordinator cleared {:?}", &path);
//             }
//         }
//         Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
//         Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
//     }
// }

// /// Returns `true` if the chunk directory for a given round height and chunk ID exists.
// /// Otherwise, returns `false`.
// fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool {
//     let path = self.chunk_directory(round_height, chunk_id);
//     Path::new(&path).exists()
// }

// /// Initializes the contribution locator file for a given round, chunk ID, and
// /// contribution ID from the coordinator.
// fn contribution_locator_init(&mut self, round_height: u64, chunk_id: u64, contribution_id: u64, _verified: bool) {
//     // If the path does not exist, attempt to initialize the file path.
//     self.chunk_directory_init(round_height, chunk_id);
//
//     let path = self.contribution_locator(round_height, chunk_id, contribution_id, false);
//     let directory = Path::new(&path).parent().expect("unable to create parent directory");
//     if !directory.exists() {
//         std::fs::create_dir_all(&path).expect("unable to create the contribution directory");
//     }
// }

// /// Returns `true` if the contribution locator for a given round height, chunk ID,
// /// and contribution ID exists. Otherwise, returns `false`.
// fn contribution_locator_exists(
//     &self,
//     round_height: u64,
//     chunk_id: u64,
//     contribution_id: u64,
//     verified: bool,
// ) -> bool {
//     let path = self.contribution_locator(round_height, chunk_id, contribution_id, verified);
//     Path::new(&path).exists()
// }

// /// Returns the round locator for a given round from the coordinator.
// fn round_locator(&self, round_height: u64) -> String {
//     // Fetch the transcript directory path.
//     let path = self.round_directory(round_height);
//
//     // Format the round locator located at `{round_directory}/output`.
//     format!("{}/output", path)
// }

// /// Returns `true` if the round locator for a given round height exists.
// /// Otherwise, returns `false`.
// fn round_locator_exists(&self, round_height: u64) -> bool {
//     let path = self.round_locator(round_height);
//     Path::new(&path).exists()
// }
// }

#[cfg(test)]
mod tests {
    use crate::{
        storage::{Disk, Storage},
        testing::prelude::*,
    };

    use std::{collections::HashMap, fs::OpenOptions, io::BufReader, path::Path};

    fn load_test() -> anyhow::Result<()> {
        let environment = &TEST_ENVIRONMENT_3;

        // Create a new instance.
        let _storage = Disk::load(&environment)?;

        // Check the base directory exists.
        let base_directory = environment.local_base_directory();
        assert!(Path::new(base_directory).exists());

        // Check the storage file path exists.
        let storage_file = format!("{}/storage.json", base_directory);
        assert!(Path::new(&storage_file).exists());

        // Open the file in read-only mode with buffer.
        let reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(&storage_file)
                .expect("unable to open the storage file"),
        );

        // Read the JSON contents of the file.
        let in_memory: HashMap<String, String> = serde_json::from_reader(reader)?;

        // Check that the storage key exists.
        assert!(in_memory.contains_key("storage"));

        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_load() {
        clear_test_transcript();
        load_test().unwrap();
    }
}

// Ok(match locator {
//     Locator::RoundHeight => "rh://".to_string(),
//     Locator::RoundState(round_height) => format!("r://{}", round_height),
//     Locator::RoundFile(round_height) => format!("rf://{}", round_height),
//     Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => format!(
//         "cf://{}.{}.{}.{}",
//         round_height, chunk_id, contribution_id, *verified as u64
//     ),
//     _ => return Err(CoordinatorError::LocatorSerializationFailed),
// })
// Ok(serde_json::to_string(locator)?)

// impl Serialize for DiskLocators {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         // serializer.serialize_str(&match self {
//         //     Locator::RoundHeight => "rh://".to_string(),
//         //     Locator::Round(round_height) => format!("r://{}", round_height),
//         //     Locator::RoundFile(round_height) => format!("rf://{}", round_height),
//         //     Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => format!(
//         //         "cf://{}.{}.{}.{}",
//         //         round_height, chunk_id, contribution_id, *verified as u64
//         //     ),
//         //     // Locator::Ping => "ping://".to_string(),
//         //     _ => return Err(ser::Error::custom("invalid serialization key")),
//         // })
//     }
// }

// impl<'de> Deserialize<'de> for DiskLocators {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         let (variant, data) = match s.splitn(2, "://").collect_tuple() {
//             Some((variant, data)) => (variant, data),
//             None => return Err(de::Error::custom("failed to parse serialization key")),
//         };
//         match (variant, data) {
//             ("rh", "") => Ok(Locator::RoundHeight),
//             ("r", value) => Ok(Locator::Round(u64::from_str(value).map_err(de::Error::custom)?)),
//             ("rf", value) => Ok(Locator::RoundFile(u64::from_str(value).map_err(de::Error::custom)?)),
//             ("cf", value) => match s.splitn(4, ".").map(u64::from_str).collect_tuple() {
//                 Some((round_height, chunk_id, contribution_id, verified)) => Ok(Locator::ContributionFile(
//                     round_height.map_err(de::Error::custom)?,
//                     chunk_id.map_err(de::Error::custom)?,
//                     contribution_id.map_err(de::Error::custom)?,
//                     verified.map_err(de::Error::custom)? as bool,
//                 )),
//                 None => Err(de::Error::custom("failed to parse serialization key")),
//             },
//             ("ping", "") => Ok(Locator::Ping),
//             _ => Err(de::Error::custom("invalid deserialization key")),
//         }
//     }
// }
