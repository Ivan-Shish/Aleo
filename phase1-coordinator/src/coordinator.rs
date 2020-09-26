use crate::{
    environment::{Environment, StorageType},
    objects::Round,
    storage::{InMemory, Key, Storage, Value},
};

use chrono::{DateTime, Utc};
use std::{
    fmt,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::{error, info};
use url::Url;

#[derive(Debug)]
pub enum CoordinatorError {
    AlreadyContributed,
    FailedToUpdateChunk,
    InvalidNumberOfChunks,
    InvalidUrl,
    Launch(rocket::error::LaunchError),
    LockAlreadyAcquired,
    MissingChunk,
    MissingContributionChunk,
    MissingVerifierIds,
    NoContributions,
    NoRound,
    PreviousRoundUnfinished,
    RoundDoesNotExist,
    RoundHeightIsZero,
    StorageFailed,
    UnauthorizedChunkContributor,
    UnauthorizedRoundContributor,
    Url(url::ParseError),
}

impl From<url::ParseError> for CoordinatorError {
    fn from(error: url::ParseError) -> Self {
        CoordinatorError::Url(error)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        Self::msg(error.to_string())
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
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
    pub fn new(environment: Environment) -> Self {
        Self {
            storage: Arc::new(RwLock::new(InMemory::load())),
            // storage: Arc::new(RwLock::new(environment.storage())),
            environment,
        }
    }

    ///
    /// Returns the current round of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// If there are no prior rounds in storage, returns `0`.
    ///
    #[inline]
    pub fn current_round(&self) -> Result<Round, CoordinatorError> {
        self.get_round(self.current_round_height()?)
    }

    ///
    /// Returns the current round height of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// For convention, a round height of `0` indicates that there have
    /// been no prior rounds of the ceremony. Thus, the first round
    /// of the ceremony has a round height of `1`.
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
    pub fn get_round(&self, height: u64) -> Result<Round, CoordinatorError> {
        // Load the round from storage.
        match self.storage()?.get(&Key::Round(height)) {
            Some(Value::Round(round)) => Ok(round.clone()),
            _ => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Loads the previous round from storage and checks that it was fully verified,
    /// then instantiates a new round and saves it to storage.
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
        info!("Starting a new round...");

        // Fetch the current height of the ceremony.
        let height = self.current_round_height()?;

        // If a prior round existed, check that it
        // has been completed and fully verified.
        if let Ok(round) = self.get_round(height) {
            if round.is_verified() {
                return Err(CoordinatorError::PreviousRoundUnfinished);
            }
        }

        // Instantiate the new round and height.
        let new_height = height + 1;
        let round = Round::new(
            self.environment.version(), /* version */
            new_height,
            started_at,
            contributor_ids,
            verifier_ids,
            chunk_verifier_ids,
            chunk_verified_base_url,
        )?;

        // TODO (howardwu): Do we need to structure this entry as an atomic transaction?
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        storage.insert(Key::Round(new_height), Value::Round(round));
        // Next, update the round height to reflect the update.
        storage.insert(Key::RoundHeight, Value::RoundHeight(new_height));

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
        let height = self.current_round_height()?;
        if height == 0 {
            return Err(CoordinatorError::RoundHeightIsZero);
        }

        // Fetch a local copy of the current round from storage.
        let mut current_round = self.get_round(height)?.clone();

        // Check that the participant is an authorized contributor to the round.
        if !self.is_current_contributor(participant_id.clone())? {
            error!("Unauthorized ({}, /chunks/{}/lock)", participant_id, chunk_id);
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

        match self
            .storage_mut()?
            .insert(Key::Round(height), Value::Round(current_round))
        {
            true => Ok(()),
            false => Err(CoordinatorError::FailedToUpdateChunk),
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
        let mut chunk = match current_round.get_chunk_mut(chunk_id) {
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
