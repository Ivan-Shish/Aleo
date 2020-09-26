use crate::{
    commands::{Aggregation, Initialization, Verification},
    environment::{Environment, StorageType},
    objects::Round,
    storage::{InMemory, Key, Storage, Value},
};

use chrono::{DateTime, Utc};
use std::{
    fmt,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::{error, info, trace, warn};
use url::Url;

#[derive(Debug)]
pub enum CoordinatorError {
    AlreadyContributed,
    ChunkAggregationFailed,
    ChunkVerificationFailed,
    Error(anyhow::Error),
    FailedToUpdateChunk,
    FullTranscriptAlreadyExists,
    InvalidNumberOfChunks,
    InvalidUrl,
    IOError(std::io::Error),
    Launch(rocket::error::LaunchError),
    LockAlreadyAcquired,
    MissingChunk,
    MissingContributionChunk,
    MissingVerifierIds,
    NoContributions,
    NoRound,
    RoundAlreadyInitialized,
    RoundChunksMissingVerification,
    RoundDoesNotExist,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundNotVerified,
    RoundSkipped,
    RoundTranscriptMissing,
    StorageFailed,
    UnauthorizedChunkContributor,
    UnauthorizedRoundContributor,
    Url(url::ParseError),
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(error: anyhow::Error) -> Self {
        CoordinatorError::Error(error)
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(error: std::io::Error) -> Self {
        CoordinatorError::IOError(error)
    }
}

impl From<url::ParseError> for CoordinatorError {
    fn from(error: url::ParseError) -> Self {
        CoordinatorError::Url(error)
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        Self::msg(error.to_string())
    }
}

/// A core structure for operating the Phase 1 ceremony.
pub struct Coordinator {
    storage: Arc<RwLock<StorageType>>,
    environment: Environment,
}

impl Coordinator {
    ///
    /// Creates a new instance of the `Coordinator`, for a given environment.
    ///
    /// The coordinator loads and instantiates an internal instance of storage.
    /// All subsequent interactions with the coordinator are directly from storage.
    ///
    /// The coordinator is forbidden from caching state about any round.
    ///
    #[inline]
    pub fn new(environment: Environment) -> Result<Self, CoordinatorError> {
        Ok(Self {
            storage: Arc::new(RwLock::new(InMemory::load()?)),
            // storage: Arc::new(RwLock::new(environment.storage())),
            environment,
        })
    }

    ///
    /// Returns the current round of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// If there are no prior rounds in storage, returns `0`.
    ///
    /// When loading the current round from storage, this function
    /// checks that the current round height matches the height
    /// set in the returned `Round` instance.
    ///
    #[inline]
    pub fn current_round(&self) -> Result<Round, CoordinatorError> {
        let round_height = self.current_round_height()?;
        let round = self.get_round(round_height)?;
        // Check that the height set in `round` matches the current round height.
        match round.get_height() == round_height {
            true => Ok(round),
            false => Err(CoordinatorError::RoundHeightMismatch),
        }
    }

    ///
    /// Returns the current round height of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// For convention, a round height of `0` indicates that there have
    /// been no prior rounds of the ceremony. The ceremony is initialized
    /// on a round height of `0` and the first round of public contribution
    /// starts on a round height of `1`.
    ///
    /// When loading the current round height from storage, this function
    /// checks that the corresponding round is in storage. Note that it
    /// only checks for the existence of a round value and does not
    /// check for its correctness.
    ///
    #[inline]
    pub fn current_round_height(&self) -> Result<u64, CoordinatorError> {
        // Acquire the storage read lock.
        let storage = self.storage()?;

        // Fetch the latest round height from storage.
        match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(height)) => match *height != 0 {
                // Case 1 - This is a typical round of the ceremony.
                // Check that the corresponding round data exists in storage.
                true => match storage.contains_key(&Key::Round(*height)) {
                    true => Ok(*height),
                    false => Err(CoordinatorError::StorageFailed),
                },
                // Case 2 - There are no prior rounds of the ceremony.
                false => Ok(0),
            },
            // Case 2 - There are no prior rounds of the ceremony.
            _ => Ok(0),
        }
    }

    ///
    /// Returns `true` if the given participant ID is included in the
    /// list of contributor IDs for the current round of the ceremony.
    ///
    /// If the contributor is not a contributor, or if there are
    /// no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant_id: String) -> Result<bool, CoordinatorError> {
        Ok(self.current_round()?.is_authorized_contributor(participant_id))
    }

    ///
    /// Returns the round corresponding to the given height from storage.
    ///
    /// If there are no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn get_round(&self, round_height: u64) -> Result<Round, CoordinatorError> {
        // Load the round corresponding to the given round height from storage.
        match self.storage()?.get(&Key::Round(round_height)) {
            Some(Value::Round(round)) => Ok(round.clone()),
            _ => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Initiates the next round of the ceremony.
    ///
    /// If there are no prior rounds in storage, this initializes a new ceremony
    /// by invoking `Initialization`, and saves it to storage.
    ///
    /// Otherwise, this loads the previous round from storage and checks that
    /// the prior round is fully verified before proceeding to the next round,
    /// and saving it in storage.
    ///
    /// In a test environment, this resets the transcript for the coordinator.
    /// In a development or production environment, this does NOT reset the
    /// transcript for the coordinator.
    ///
    /// On success, the function returns the new round height.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn next_round(
        &mut self,
        started_at: DateTime<Utc>,
        contributor_ids: &Vec<String>,
        verifier_ids: &Vec<String>,
        chunk_verifier_ids: &Vec<String>,
        chunk_verified_base_url: &Vec<&str>,
    ) -> Result<u64, CoordinatorError> {
        info!("Starting transition to new round");

        // Fetch the current height of the ceremony.
        let round_height = self.current_round_height()?;
        trace!("Current round height from storage is {}", round_height);

        // Attempt to fetch the current round directly.
        let mut current_round = self.get_round(round_height);
        trace!("Current round exists in storage - {}", current_round.is_ok());

        // If this is the initial round of the ceremony, this function
        // proceeds to run ceremony initialization.
        if round_height == 0 && current_round.is_err() {
            trace!("Starting ceremony initialization");

            // TODO (howardwu): Switch out the contributor IDs and verifier IDs for null and self.
            // Execute round initialization.
            let round = self.initialize_round(
                round_height,
                started_at,
                contributor_ids,
                verifier_ids,
                chunk_verifier_ids,
                chunk_verified_base_url,
            )?;
            // Fetch the new round height, which should still be 0 in this case.
            let new_height = round.get_height();
            trace!("Round height after initialization is set to {}", new_height);

            info!("Completed initialization for round {}", new_height);

            // TODO (howardwu): Add a for loop here to iterate through all of the chunks and verify them.

            current_round = Ok(round);
        }

        // Check that all chunks in the current round are verified,
        // so that we may transition to the next round.
        if !current_round?.are_chunks_verified() {
            return Err(CoordinatorError::RoundNotVerified);
        }

        // Execute round aggregation and aggregate verification.
        let round = self.aggregate_round(
            round_height,
            started_at,
            contributor_ids,
            verifier_ids,
            chunk_verifier_ids,
            chunk_verified_base_url,
        )?;
        // Fetch the new round height.
        let new_height = round.get_height();

        info!("Completed transition from round {} to {}", round_height, new_height);
        Ok(new_height)
    }

    /// TODO (howardwu): Should we abstract the chunk locator as a ChunkLocator struct
    ///  in order to generalize the construction of the locator?
    ///
    /// Returns the chunk locator for a given round height
    /// and given chunk ID.
    ///
    #[inline]
    pub fn get_chunk_locator(&self, round_height: u64, chunk_id: u64) -> Result<Url, CoordinatorError> {
        // Fetch the round corresponding to the given round height.
        let round = self.get_round(round_height)?;
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.get_chunk(chunk_id)?;
        // Parse and return the chunk locator.
        let base_url = self.environment.base_url();
        let version = chunk.version();
        match format!("{}/chunks/{}/contribution/{}", base_url, chunk_id, version).parse() {
            Ok(locator) => Ok(locator),
            _ => {
                error!("Failed to parse a locator ({}, {}, {})", base_url, chunk_id, version);
                return Err(CoordinatorError::InvalidUrl);
            }
        }
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID from storage
    /// for a given participant ID.
    ///
    /// On success, the function returns `Ok(())`.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn lock_chunk(&self, chunk_id: u64, participant_id: String) -> Result<(), CoordinatorError> {
        // Check that the ceremony started and exists in storage.
        let round_height = self.current_round_height()?;
        if round_height == 0 {
            return Err(CoordinatorError::RoundHeightIsZero);
        }

        // Fetch a local copy of the current round from storage.
        let mut current_round = self.get_round(round_height)?.clone();

        // Check that the participant is an authorized contributor to the round.
        if !self.is_current_contributor(participant_id.clone())? {
            error!("{} cannot acquire lock on chunk {}", participant_id, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Check that the participant does not currently hold a lock to any chunk.
        if current_round
            .get_chunks()
            .iter()
            .filter(|chunk| chunk.is_locked_by(&participant_id))
            .next()
            .is_some()
        {
            error!("{} already holds the lock on chunk {}", participant_id, chunk_id);
            return Err(CoordinatorError::LockAlreadyAcquired);
        }

        // Fetch the chunk the participant intends to contribute to.
        let mut chunk = current_round.get_chunk(chunk_id)?.clone();

        // Attempt to acquire the lock for the given participant ID.
        chunk.acquire_lock(&participant_id)?;

        // Attempt to update the current round chunk.
        if !current_round.update_chunk(chunk_id, &chunk) {
            return Err(CoordinatorError::FailedToUpdateChunk);
        }

        // If the lock acquisition succeeded, insert and save the updated round into storage.
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the updated round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(current_round)) {
            // Next, save the round to storage.
            if storage.save() {
                success = true;
            }
        }
        match success {
            true => Ok(()),
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    ///
    /// Attempts to contribute a contribution to a given chunk ID
    /// for a given participant ID.
    ///
    /// On success, the function returns the chunk locator.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn contribute_chunk(&self, chunk_id: u64, participant_id: String) -> Result<Url, CoordinatorError> {
        info!("Attempting to contribute to a chunk");

        // Check that the ceremony started and exists in storage.
        let height = self.current_round_height()?;
        if height == 0 {
            error!("The ceremony has not started");
            return Err(CoordinatorError::RoundHeightIsZero);
        }

        // Fetch a local copy of the current round from storage.
        let mut current_round = self.get_round(height)?.clone();

        // Check that the participant is an authorized contributor to the round.
        if !self.is_current_contributor(participant_id.clone())? {
            error!("Unauthorized ({}, /chunks/{}/lock)", participant_id, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Fetch the chunk the participant intends to contribute to.
        let chunk = match current_round.get_chunk_mut(chunk_id) {
            Some(chunk) => chunk,
            None => return Err(CoordinatorError::MissingChunk),
        };

        // Check that the participant acquired the lock.
        if !chunk.is_locked_by(&participant_id) {
            error!("{} should have lock on chunk {} but does not", participant_id, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Fetch the chunk locator for the current round height.
        let locator = self.get_chunk_locator(height, chunk_id)?;

        // TODO (howardwu): Make a dedicated endpoint and method for checking verifiers.
        // match current_round.verifier_ids.contains(&participant_id) {
        //     true => match chunk.contributions.get_mut(index) {
        //         Some(contribution) => {
        //             contribution.verifier_id = Some(participant_id);
        //             contribution.verified_location = Some(location.clone());
        //             contribution.verified = true;
        //         }
        //         None => return Err(CoordinatorError::MissingContributionChunk),
        //     },
        //     false => ,
        // };

        chunk.add_contribution(chunk_id, participant_id, &locator.to_string())?;

        Ok(locator)
    }

    /// Attempts to run initialization for a given round.
    #[inline]
    fn initialize_round(
        &self,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: &Vec<String>,
        verifier_ids: &Vec<String>,
        chunk_verifier_ids: &Vec<String>,
        chunk_verified_base_url: &Vec<&str>,
    ) -> Result<Round, CoordinatorError> {
        trace!("Starting initialization for round {}", round_height);

        // Fetch the current round height.
        let current_round_height = self.current_round_height()?;

        // Check that the given round height is above the current round height.
        // Depending on the case, this function may safely assume that the
        // current round height must be AT LEAST less than the current round height.
        if round_height < current_round_height {
            error!("Round {} is less than round {}", round_height, current_round_height);
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Attempt to load the round corresponding to the given round height from storage.
        // If there is no round in storage, proceed to create a new round instance.
        let round = match self.storage()?.get(&Key::Round(round_height)) {
            // Check that the round does not exist in storage.
            // If it exists, this means the round was already initialized.
            Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
            Some(_) => return Err(CoordinatorError::StorageFailed),
            // Create a new round instance.
            _ => Round::new(
                self.environment.version(),
                round_height,
                started_at,
                contributor_ids,
                verifier_ids,
                chunk_verifier_ids,
                chunk_verified_base_url,
            )?,
        };
        // Check that the height specified in the round matches the given round height.
        if round.get_height() != round_height {
            return Err(CoordinatorError::RoundSkipped);
        }

        // If the path exists, this means a prior ceremony is stored as a transcript.
        let transcript_directory = self.environment.transcript_directory(round_height);
        let path = Path::new(&transcript_directory);
        if path.exists() {
            // If this is a test environment, attempt to clear it for the coordinator.
            if let Environment::Test(_) = self.environment {
                warn!("Coordinator is clearing {:?}", &path);
                std::fs::remove_dir_all(&path).expect("unable to remove transcript directory");
                warn!("Coordinator cleared {:?}", &path);
            }
        }
        // Execute initialization on all chunks to start a new ceremony.
        for chunk_id in 0..self.environment.number_of_chunks() {
            info!("Coordinator is starting initialization");
            // TODO (howardwu): Add contribution hash to `Round`.
            let _contribution_hash = Initialization::run(&self.environment, round_height, chunk_id)?;
            info!("Coordinator completed initialization");
        }

        // If the initialization succeeded, insert and save the round into storage.
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round.clone())) {
            // Next, update the round height to reflect the update.
            if storage.insert(Key::RoundHeight, Value::RoundHeight(round_height)) {
                // Lastly, save the round to storage.
                if storage.save() {
                    success = true;
                }
            }
        }
        match success {
            true => Ok(round),
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    /// Attempts to run verification for a given round.
    #[inline]
    fn aggregate_round(
        &self,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: &Vec<String>,
        verifier_ids: &Vec<String>,
        chunk_verifier_ids: &Vec<String>,
        chunk_verified_base_url: &Vec<&str>,
    ) -> Result<Round, CoordinatorError> {
        // Fetch the current round.
        let current_round = self.current_round()?;

        // Check that the given round height matches the height value in the current round.
        if round_height != current_round.get_height() {
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // TODO (howardwu): Implement verify_chunk and remove this.
        // Execute verification on all chunks to check that the
        // current round has been completed and fully verified.
        for chunk_id in 0..self.environment.number_of_chunks() {
            info!("Coordinator is starting verification");
            Verification::run(&self.environment, round_height, chunk_id)?;
            info!("Coordinator completed verification");
        }

        // Check that all current round chunks are verified.
        if !current_round.are_chunks_verified() {
            return Err(CoordinatorError::RoundChunksMissingVerification);
        }

        // Check that the transcript directory corresponding to this round exists.
        let transcript_directory = self.environment.transcript_directory(round_height);
        let path = Path::new(&transcript_directory);
        if !path.exists() {
            return Err(CoordinatorError::RoundTranscriptMissing);
        }

        // TODO (howardwu): Add aggregate verification logic.
        // Execute aggregation to combine on all chunks in preparation for the next round.
        info!("Coordinator is starting aggregation");
        Aggregation::run(&self.environment, round_height)?;
        info!("Coordinator completed aggregation");

        // Instantiate the new round and height.
        let new_height = round_height + 1;
        let new_round = Round::new(
            self.environment.version(),
            new_height,
            started_at,
            contributor_ids,
            verifier_ids,
            chunk_verifier_ids,
            chunk_verified_base_url,
        )?;

        // TODO (howardwu): Do we need to structure this entry as an atomic transaction?
        // If the transition succeeded, insert and save the round into storage.
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        if storage.insert(Key::Round(new_height), Value::Round(new_round.clone())) {
            // Next, update the round height to reflect the update.
            if storage.insert(Key::RoundHeight, Value::RoundHeight(new_height)) {
                // Lastly, save the round to storage.
                if storage.save() {
                    info!("Completed transition to new round");
                    success = true;
                }
            }
        }
        match success {
            true => Ok(new_round),
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    // /// Attempts to load a round contribution into storage.
    // #[inline]
    // fn load_round(&self, round_height: u64) -> Result<RwLockReadGuard<StorageType>, CoordinatorError> {
    //     // Open the transcript file for the given round height.
    //     let transcript = environment.final_transcript_locator(round_height);
    //     let file = OpenOptions::new().read(true).open(&transcript)?;
    //     let reader = unsafe { MmapOptions::new().map(&file)? };
    //
    //     let chunk_size = 1 << 30; // read by 1GB from map
    //     for chunk in input_map.chunks(chunk_size) {
    //         hasher.input(&chunk);
    //     }
    //
    //     match self.storage.read() {
    //         Ok(storage) => Ok(storage),
    //         _ => Err(CoordinatorError::StorageFailed),
    //     }
    // }

    /// Attempts to acquire the read lock for storage.
    #[inline]
    fn storage(&self) -> Result<RwLockReadGuard<StorageType>, CoordinatorError> {
        match self.storage.read() {
            Ok(storage) => Ok(storage),
            _ => Err(CoordinatorError::StorageFailed),
        }
    }

    /// Attempts to acquire the write lock for storage.
    #[inline]
    fn storage_mut(&self) -> Result<RwLockWriteGuard<StorageType>, CoordinatorError> {
        match self.storage.write() {
            Ok(storage) => Ok(storage),
            _ => Err(CoordinatorError::StorageFailed),
        }
    }
}
