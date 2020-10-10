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
    io::Write,
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
            let path = storage.to_path(locator)?;
            trace!("Loading {}", path);

            let file = OpenOptions::new().read(true).write(true).open(&path)?;
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
    #[inline]
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
        trace!("Setting {} to {} bytes", self.to_path(&locator)?, size);
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
    #[inline]
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
    fn to_locator(&self, path: &str) -> Result<Locator, CoordinatorError> {
        self.locator_path.to_locator(path)
    }
}

impl StorageObject for Disk {
    /// Returns an object reader for the given locator.
    #[inline]
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
    #[inline]
    fn writer(&self, locator: &Locator) -> Result<ObjectWriter, CoordinatorError> {
        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            trace!("Locator missing in call to writer() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Acquire the file read lock.
        let writer = self
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
                debug!("File size of {} is {}", self.to_path(locator)?, found);
                if found != expected {
                    error!("Contribution file size should be {} but found {}", expected, found);
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }
                Ok(writer)
            }
            Locator::ContributionFile(_, chunk_id, _, verified) => {
                // Check that the contribution size is correct.
                let expected = Object::contribution_file_size(&self.environment, *chunk_id, *verified);
                let found = self.size(&locator)?;
                debug!("File size of {} is {}", self.to_path(locator)?, found);
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
    resolver: DiskLocator,
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

        // Create the resolver.
        let resolver = DiskLocator {
            base: base_directory.to_string(),
        };

        // Check that the storage file exists. If not, create a new storage file.
        let locators = match !Path::new(&resolver.manifest()).exists() {
            // Create and store a new instance of `DiskManifest`.
            true => {
                // Serialize the paths.
                let paths: Vec<String> = vec![];
                let serialized = serde_json::to_string_pretty(&paths)?;

                // Write the serialized paths to the manifest.
                fs::write(Path::new(&resolver.manifest()), serialized)?;

                HashSet::default()
            }
            false => {
                // Read the serialized paths from the manifest.
                let serialized = fs::read_to_string(&Path::new(&resolver.manifest()))?;

                // Convert all paths to locators.
                let paths: Vec<String> = serde_json::from_str(&serialized)?;
                let locators: HashSet<Locator> = paths
                    .par_iter()
                    .map(|path| resolver.to_locator(&path).unwrap())
                    .collect();

                locators
            }
        };

        Ok(Self { locators, resolver })
    }

    #[inline]
    fn save(&mut self) -> Result<(), CoordinatorError> {
        // Convert all locators to paths.
        let mut paths: Vec<String> = self
            .locators
            .par_iter()
            .map(|locator| self.resolver.to_path(&locator).unwrap())
            .collect();

        // Sort the list
        paths.par_sort();

        // Serialize the paths.
        let serialized = serde_json::to_string_pretty(&paths)?;

        // Write the serialized paths to the manifest.
        fs::write(Path::new(&self.resolver.manifest()), serialized)?;

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
    fn to_locator(&self, path: &str) -> Result<Locator, CoordinatorError> {
        // Sanitize the given path and base to the local OS.
        let path = Path::new(path);
        let base = Path::new(&self.base);

        // Check that the path matches the expected base.
        if !path.starts_with(base) {
            error!("{:?} does not start with {:?}", path, base);
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
                                /* In chunk directory */

                                // Check if it matches the contribution file.
                                if path.starts_with("contribution_") {
                                    let (id, extension) = path
                                        .strip_prefix("contribution_")
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
    /// Returns the storage manifest file path.
    #[inline]
    fn manifest(&self) -> String {
        format!("{}/manifest.json", self.base)
    }

    /// Returns the round directory for a given round height from the coordinator.
    #[inline]
    fn round_directory(&self, round_height: u64) -> String {
        format!("{}/round_{}", self.base, round_height)
    }

    /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    #[inline]
    fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        // Fetch the transcript directory path.
        let path = self.round_directory(round_height);

        // Format the chunk directory as `{round_directory}/chunk_{chunk_id}`.
        format!("{}/chunk_{}", path, chunk_id)
    }

    /// Initializes the chunk directory for a given  round height, and chunk ID.
    #[inline]
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
    #[inline]
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
