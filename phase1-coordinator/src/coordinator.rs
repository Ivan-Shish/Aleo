use crate::{
    commands::{Aggregation, Initialization, Verification},
    environment::{Environment, StorageType},
    objects::{Participant, Round},
    storage::{InMemory, Key, Storage, Value},
};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use std::{
    fmt,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::{error, info, trace, warn};
use url::Url;

#[derive(Debug)]
pub enum CoordinatorError {
    ChunkAlreadyVerified,
    ChunkIdMismatch,
    ChunkLockAlreadyAcquired,
    ChunkMissing,
    ChunkMissingTranscript,
    ChunkMissingVerification,
    ChunkNotLocked,
    ChunkNotLockedOrWrongParticipant,
    ChunkUpdateFailed,
    ChunkVerificationFailed,
    ChunkVerifierMissing,
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFileSizeMismatch,
    ContributionIdMismatch,
    ContributionMissing,
    ContributionMissingVerification,
    ContributionMissingVerifiedLocator,
    ContributionMissingVerifier,
    ContributionVerificationFailed,
    ContributorAlreadyContributed,
    ExpectedContributor,
    ExpectedVerifier,
    Error(anyhow::Error),
    FinalTranscriptAlreadyExists,
    InvalidUrl,
    IOError(std::io::Error),
    Launch(rocket::error::LaunchError),
    MissingVerifierIds,
    NumberOfChunksInvalid,
    NumberOfChunkVerifierIdsInvalid,
    NumberOfChunkVerifiedBaseUrlsInvalid,
    NumberOfContributionsDiffer,
    RoundAggregationFailed,
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
    UnauthorizedChunkVerifier,
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
    /// Returns `true` if the given participant is a contributor and included
    /// in the list of contributors for the current round of the ceremony.
    ///
    /// If the contributor is not a contributor, or if there are
    /// no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant: &Participant) -> Result<bool, CoordinatorError> {
        match participant {
            Participant::Contributor(_) => Ok(self.current_round()?.is_authorized_contributor(participant)),
            Participant::Verifier(_) => Err(CoordinatorError::ExpectedContributor),
        }
    }

    ///
    /// Returns `true` if the given participant is a verifier and included
    /// in the list of verifiers for the current round of the ceremony.
    ///
    /// If the contributor is not a contributor, or if there are
    /// no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn is_current_verifier(&self, participant: &Participant) -> Result<bool, CoordinatorError> {
        match participant {
            Participant::Contributor(contributor_id) => Err(CoordinatorError::ExpectedVerifier),
            Participant::Verifier(_) => Ok(self.current_round()?.is_authorized_verifier(participant)),
        }
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
    /// Updates the round corresponding to the given height from storage.
    ///
    #[inline]
    fn set_round(&self, round_height: u64, round: Round) -> Result<(), CoordinatorError> {
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round)) {
            // Next, update the round height to reflect the update.
            if storage.insert(Key::RoundHeight, Value::RoundHeight(round_height)) {
                // Lastly, save the round to storage.
                if storage.save() {
                    success = true;
                }
            }
        }
        match success {
            true => {
                trace!("Writing round {} to storage", round_height);
                Ok(())
            }
            false => Err(CoordinatorError::StorageFailed),
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
        contributors: Vec<Participant>,
        verifiers: Vec<Participant>,
        chunk_verifiers: Vec<Participant>,
        chunk_verified_base_url: Vec<&str>,
    ) -> Result<u64, CoordinatorError> {
        // Fetch the current height of the ceremony.
        let round_height = self.current_round_height()?;
        trace!("Current round height from storage is {}", round_height);

        // Attempt to fetch the current round directly.
        let mut current_round = self.get_round(round_height);
        trace!("Current round exists in storage - {}", current_round.is_ok());

        // If this is the initial round of the ceremony, this function
        // proceeds to run ceremony initialization.
        if round_height == 0 && current_round.is_err() {
            info!("Starting initialization and verification");

            // Execute the round initialization and verification as the coordinator.
            // On success, the new round will have been saved to storage.
            let round = self.initialize_round(round_height, started_at, chunk_verified_base_url.clone())?;

            // Check the new round height, which should still be 0 in this case.
            if round_height != round.get_height() {
                error!("Round height after initialization was set to {}", round.get_height());
                return Err(CoordinatorError::RoundHeightMismatch);
            }

            info!("Completed initialization and verification for round {}", round_height);
            current_round = Ok(round);
        }

        // Check that all chunks in the current round are verified,
        // so that we may transition to the next round.
        let current_round = current_round?;
        if !&current_round.is_complete() {
            error!("Round {} is not complete and next round is not starting", round_height);
            trace!("{:#?}", &current_round);
            return Err(CoordinatorError::RoundNotVerified);
        }

        info!("Starting transition to new round");

        // Execute round aggregation and aggregate verification.
        let round = self.aggregate_round(
            round_height,
            started_at,
            contributors,
            verifiers,
            chunk_verifiers,
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
    /// for a given participant.
    ///
    /// On success, the function returns `Ok(())`.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_lock_chunk(&self, chunk_id: u64, participant: Participant) -> Result<(), CoordinatorError> {
        // Check that the ceremony started and exists in storage.
        let round_height = self.current_round_height()?;

        // Fetch a local copy of the current round from storage.
        let mut current_round = self.get_round(round_height)?.clone();

        // If the participant is a contributor ID, check they are authorized to acquire the lock as a contributor.
        if participant.is_contributor() {
            // Check that the contributor is an authorized contributor in the current round.
            if !self.is_current_contributor(&participant)? {
                error!("{} is not an authorized contributor", &participant);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the contributor does not currently hold a lock to any chunk.
            if current_round
                .get_chunks()
                .iter()
                .filter(|chunk| chunk.is_locked_by(&participant))
                .next()
                .is_some()
            {
                error!("{} already holds the lock on chunk {}", &participant, chunk_id);
                return Err(CoordinatorError::ChunkLockAlreadyAcquired);
            }
        }

        // If the participant is a verifier ID, check they are authorized to acquire the lock as a verifier.
        if participant.is_verifier() {
            // Check that the verifier is an authorized verifier in the current round.
            if !self.is_current_verifier(&participant)? {
                error!("{} is not an authorized verifier", &participant);
                return Err(CoordinatorError::UnauthorizedChunkVerifier);
            }
        }

        // Fetch the chunk the participant intends to contribute to,
        let chunk = current_round.get_chunk_mut(chunk_id)?;

        // Attempt to acquire the lock for the given participant ID.
        chunk.acquire_lock(participant)?;

        // As the lock acquisition succeeded, insert and save the updated round into storage.
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
    pub fn contribute_chunk(&self, chunk_id: u64, participant: Participant) -> Result<Url, CoordinatorError> {
        info!("Attempting to add contribution to a chunk");

        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Check that the ceremony started and exists in storage.
        let height = self.current_round_height()?;
        if height == 0 {
            error!("The ceremony has not started");
            return Err(CoordinatorError::RoundHeightIsZero);
        }

        // Fetch a local copy of the current round from storage.
        let mut current_round = self.get_round(height)?.clone();

        // Check that the participant is an authorized contributor to the round.
        if !self.is_current_contributor(&participant)? {
            error!("Unauthorized ({}, /chunks/{}/lock)", &participant, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Fetch the chunk the participant intends to contribute to.
        let chunk = current_round.get_chunk_mut(chunk_id)?;

        // Check that the participant acquired the lock.
        if !chunk.is_locked_by(&participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
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
        //         None => return Err(CoordinatorError::ContributionMissing),
        //     },
        //     false => ,
        // };

        chunk.add_contribution(participant, &locator.to_string())?;

        Ok(locator)
    }

    // /// Attempts to run verification in the current round for a given chunk ID.
    // #[inline]
    // fn verify_chunk(&self, chunk_id: u64) -> Result<(), CoordinatorError> {
    //     // Fetch the current round.
    //     let mut current_round = self.current_round()?;
    //     let round_height = current_round.get_height();
    //
    //     // Check that the chunk transcript directory corresponding to this round exists.
    //     let chunk_directory = self.environment.chunk_directory(round_height, chunk_id);
    //     if !Path::new(&chunk_directory).exists() {
    //         return Err(CoordinatorError::ChunkMissingTranscript);
    //     }
    //
    //     for contribution_id in 0..current_round {}
    //
    //     self.verify_contribution(chunk_id, contribution_id)
    // }

    /// Attempts to run initialization for a given round.
    #[inline]
    fn initialize_round(
        &self,
        round_height: u64,
        started_at: DateTime<Utc>,
        chunk_verified_base_url: Vec<&str>,
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

        // Initialize the contributors as a list comprising only the coorodinator contributor,
        // as this is for initialization.
        let contributors = vec![self.environment.coordinator_contributor()];

        // Initialize the verifiers as a list comprising only the coordinator verifier,
        // as this is for initialization.
        let verifiers = vec![self.environment.coordinator_verifier()];

        // Initialize the chunk verifiers as a list comprising only the coordinator verifier,
        // as this is for initialization.
        let chunk_verifiers = (0..self.environment.number_of_chunks())
            .into_par_iter()
            .map(|_| self.environment.coordinator_verifier())
            .collect::<Vec<_>>();

        // Attempt to load the round corresponding to the given round height from storage.
        // If there is no round in storage, proceed to create a new round instance.
        let mut round = match self.storage()?.get(&Key::Round(round_height)) {
            // Check that the round does not exist in storage.
            // If it exists, this means the round was already initialized.
            Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
            Some(_) => return Err(CoordinatorError::StorageFailed),
            // Create a new round instance.
            _ => Round::new(
                &self.environment,
                round_height,
                started_at,
                contributors,
                verifiers,
                chunk_verifiers,
                chunk_verified_base_url,
            )?,
        };
        // Check that the height specified in the round matches the given round height.
        if round.get_height() != round_height {
            return Err(CoordinatorError::RoundSkipped);
        }

        // If the path exists, this means a prior ceremony is stored as a transcript.
        let round_directory = self.environment.round_directory(round_height);
        let path = Path::new(&round_directory);
        if path.exists() {
            // If this is a test environment, attempt to clear it for the coordinator.
            if let Environment::Test(_) = self.environment {
                warn!("Coordinator is clearing {:?}", &path);
                std::fs::remove_dir_all(&path).expect("unable to remove transcript directory");
                warn!("Coordinator cleared {:?}", &path);
            }
        }

        // Write the new round to storage.
        self.set_round(round_height, round.clone())?;

        // Execute initialization on all chunks to start a new ceremony.
        for chunk_id in 0..self.environment.number_of_chunks() {
            // Read the updated round to storage.
            round = self.get_round(round_height)?;

            info!("Coordinator is starting initialization on chunk {}", chunk_id);
            // TODO (howardwu): Add contribution hash to `Round`.
            let _contribution_hash = Initialization::run(&self.environment, round_height, chunk_id)?;
            info!("Coordinator completed initialization on chunk {}", chunk_id);

            // Check that the contribution locator corresponding to this round and chunk now exists.
            let contribution_locator = self.environment.contribution_locator(round_height, chunk_id, 0);
            if !Path::new(&contribution_locator).exists() {
                return Err(CoordinatorError::RoundTranscriptMissing);
            }

            // Write the updated round to storage.
            self.set_round(round_height, round.clone())?;

            // Fetch the verifier of the coordinator and attempt to
            // acquire the chunk lock for verification.
            let verifier = self.environment.coordinator_verifier();
            self.try_lock_chunk(chunk_id, verifier.clone())?;

            // Read the updated round to storage.
            round = self.get_round(round_height)?;

            info!("Coordinator is starting verification on chunk {}", chunk_id);
            Verification::run(&self.environment, round_height, chunk_id, 0)?;
            info!("Coordinator completed verification on chunk {}", chunk_id);

            // Attempts to set the current contribution as verified in the current round.
            round.verify_contribution(chunk_id, 0, &verifier)?;

            // Write the updated round to storage.
            self.set_round(round_height, round.clone())?;
        }

        Ok(round)
    }

    /// Attempts to run verification in the current round for a given chunk ID and contribution ID.
    #[inline]
    fn verify_contribution(&self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Fetch the current round.
        let mut current_round = self.current_round()?;
        let round_height = current_round.get_height();

        // Check that the contribution locator corresponding to this round and chunk exists.
        let contribution_locator = self
            .environment
            .contribution_locator(round_height, chunk_id, contribution_id);
        if !Path::new(&contribution_locator).exists() {
            return Err(CoordinatorError::RoundTranscriptMissing);
        }

        // Fetch the verifier of the coordinator and attempt to
        // acquire the chunk lock for verification.
        let verifier = self.environment.coordinator_verifier();
        self.try_lock_chunk(chunk_id, verifier.clone())?;

        info!("Coordinator is starting chunk verification");
        Verification::run(&self.environment, round_height, chunk_id, contribution_id)?;
        info!("Coordinator completed chunk verification");

        // Attempts to set the current contribution as verified in the current round.
        current_round.verify_contribution(chunk_id, contribution_id, &verifier)?;

        // TODO (howardwu): Do we need to structure this entry as an atomic transaction?
        // If the transition succeeded, insert and save the round into storage.
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(current_round.clone())) {
            // Next, save the round to storage.
            if storage.save() {
                info!("Completed transition to new round");
                success = true;
            }
        }
        match success {
            true => Ok(()),
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    /// Attempts to run aggregation for a given round.
    #[inline]
    fn aggregate_round(
        &self,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributors: Vec<Participant>,
        verifiers: Vec<Participant>,
        chunk_verifiers: Vec<Participant>,
        chunk_verified_base_url: Vec<&str>,
    ) -> Result<Round, CoordinatorError> {
        // Fetch the current round.
        let current_round = self.current_round()?;

        // Check that the given round height matches the height value in the current round.
        if round_height != current_round.get_height() {
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // Check that all current round chunks are verified.
        if !current_round.is_complete() {
            return Err(CoordinatorError::RoundChunksMissingVerification);
        }

        // TODO (howardwu): Do pre-check that all current chunk contributions are present
        // Check that the transcript directory corresponding to this round exists.
        let round_directory = self.environment.round_directory(round_height);
        let path = Path::new(&round_directory);
        if !path.exists() {
            return Err(CoordinatorError::RoundTranscriptMissing);
        }

        // TODO (howardwu): Add aggregate verification logic.
        // Execute aggregation to combine on all chunks in preparation for the next round.
        info!("Coordinator is starting aggregation");
        Aggregation::run(&self.environment, &current_round)?;
        info!("Coordinator completed aggregation");

        // Instantiate the new round and height.
        let new_height = round_height + 1;
        let new_round = Round::new(
            &self.environment,
            new_height,
            started_at,
            contributors,
            verifiers,
            chunk_verifiers,
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
