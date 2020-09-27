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
use tracing::{debug, error, info, trace, warn};
use url::Url;

#[derive(Debug)]
pub enum CoordinatorError {
    ChunkAlreadyComplete,
    ChunkAlreadyVerified,
    ChunkIdMismatch,
    ChunkLockAlreadyAcquired,
    ChunkMissing,
    ChunkMissingTranscript,
    ChunkMissingVerification,
    ChunkNotLocked,
    ChunkNotLockedOrByWrongParticipant,
    ChunkUpdateFailed,
    ChunkVerifierMissing,
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFileSizeMismatch,
    ContributionIdIsNonzero,
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
    FinalRoundTranscriptMissing,
    FinalTranscriptAlreadyExists,
    InitializationFailed,
    InitializationTranscriptsDiffer,
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
    RoundNotComplete,
    RoundNotVerified,
    RoundSkipped,
    RoundTranscriptMissing,
    StorageFailed,
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    Url(url::ParseError),
    VerificationFailed,
    VerificationOnContributionIdZero,
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
    /// Returns a reference to the current round of the ceremony
    /// from storage, irrespective of the stage of its completion.
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
    /// Returns a reference to the round corresponding to the given height from storage.
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
        trace!("Writing round {} to storage", round_height);

        // TODO (howardwu): Do we need to structure this entry as an atomic transaction?
        let mut success = false;
        // Acquire the storage write lock.
        let mut storage = self.storage_mut()?;
        // First, add the new round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round)) {
            // Next, update the round height to reflect the update.
            if storage.insert(Key::RoundHeight, Value::RoundHeight(round_height)) {
                // Lastly, save the round to storage.
                if storage.save() {
                    debug!("Completed writing round {} to storage", round_height);
                    success = true;
                }
            }
        }
        match success {
            true => Ok(()),
            false => Err(CoordinatorError::StorageFailed),
        }
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
        let round_height = self.current_round_height()?;

        // Load the round corresponding to the given round height from storage.
        let mut storage = self.storage_mut()?;
        let round = match storage.get_mut(&Key::Round(round_height)) {
            Some(Value::Round(round)) => round,
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the height set in `round` matches the current round height.
        if round.get_height() != round_height {
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // Attempt to lock the given chunk ID for participant.
        round.try_lock_chunk(chunk_id, participant)?;

        Ok(())
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

    ///
    /// Initiates the next round of the ceremony.
    ///
    /// If there are no prior rounds in storage, this initializes a new ceremony
    /// by invoking `Initialization`, and saves it to storage.
    ///
    /// Otherwise, this loads the current round from storage and checks that
    /// it is fully verified before proceeding to aggregate the round, and
    /// initialize the next round, saving it to storage for the coordinator.
    ///
    /// In a test environment, this function resets the transcript for the
    /// coordinator when round height is 0.
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
        chunk_verified_base_url: Vec<String>,
    ) -> Result<u64, CoordinatorError> {
        // Fetch the current height of the ceremony.
        let round_height = self.current_round_height()?;
        trace!("Current round height from storage is {}", round_height);

        let next_height = round_height + 1;
        info!("Starting transition from round {} to {}", round_height, next_height);

        // Execute aggregation of the current round in preparation for
        // transition to next round. If this is the initial round, there
        // should be nothing to aggregate and we may continue.
        if round_height != 0 {
            // Attempt to fetch the current round directly.
            let mut current_round = self.get_round(round_height);
            trace!("Check current round exists in storage ({})", current_round.is_ok());

            // Check that all chunks in the current round are verified,
            // so that we may transition to the next round.
            let current_round = current_round?;
            if !&current_round.is_complete() {
                error!("Round {} is not complete and next round is not starting", round_height);
                trace!("{:#?}", &current_round);
                return Err(CoordinatorError::RoundNotComplete);
            }

            // Execute round aggregation and aggregate verification on the current round.
            self.aggregate_current_round()?;
        }

        // Execute the round initialization as the coordinator.
        // On success, the new round will have been saved to storage.
        self.initialize_round(
            round_height,
            started_at,
            contributors,
            verifiers,
            chunk_verifiers,
            chunk_verified_base_url.clone(),
        )?;

        // Fetch the new round height.
        let new_height = self.current_round_height()?;

        // Check that the new height increments the prior round height by 1.
        if new_height != next_height {
            error!("Round height after initialization is {}", new_height);
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        info!("Completed transition from round {} to {}", round_height, new_height);
        Ok(new_height)
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

    ///
    /// Attempts to run initialization for a given round.
    ///
    /// In a test environment, this function clears the transcript for the
    /// coordinator. In a development or production environment, this
    /// does NOT reset the transcript for the coordinator.
    ///
    #[inline]
    fn initialize_round(
        &self,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributors: Vec<Participant>,
        verifiers: Vec<Participant>,
        chunk_verifiers: Vec<Participant>,
        chunk_verified_base_url: Vec<String>,
    ) -> Result<(), CoordinatorError> {
        trace!("Received call to initialize round {}", round_height);

        // Fetch the current round height.
        let current_round_height = self.current_round_height()?;

        // Check that the given round height is above the current round height.
        if round_height < current_round_height {
            error!("Round {} is less than round {}", round_height, current_round_height);
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }
        // Check that the given round height corresponds to the current round height.
        if round_height != current_round_height {
            error!("Expected round height {} == {}", round_height, current_round_height);
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // If this is the initial round, ensure the round does not exist yet.
        // Attempt to load the round corresponding to the given round height from storage.
        // If there is no round in storage, proceed to create a new round instance,
        // and run `Initialization` to start the ceremony.
        if round_height == 0 {
            // If the path exists, this means a prior *ceremony* is stored as a transcript.
            let round_directory = self.environment.round_directory(round_height);
            let path = Path::new(&round_directory);
            if path.exists() {
                // If this is a test environment, attempt to clear it for the coordinator.
                if let Environment::Test(_) = self.environment {
                    warn!("Coordinator is clearing {:?}", &path);
                    std::fs::remove_dir_all(&path).expect("unable to remove transcript directory");
                    warn!("Coordinator cleared {:?}", &path);
                } else {
                    return Err(CoordinatorError::RoundAlreadyInitialized);
                }
            }

            // Create an instantiation of `Round` for round 0.
            let round = {
                // Initialize the contributors as a list comprising only the coordinator contributor,
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

                // Initialize the chunk verifiers as a list comprising only the coordinator verifier,
                // as this is for initialization.
                let chunk_verified_locators = (0..self.environment.number_of_chunks())
                    .into_par_iter()
                    .map(|chunk_id| self.environment.contribution_locator(round_height, chunk_id, 0))
                    .collect::<Vec<_>>();

                match self.storage()?.get(&Key::Round(round_height)) {
                    // Check that the round does not exist in storage.
                    // If it exists, this means the round was already initialized.
                    Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
                    Some(_) => return Err(CoordinatorError::StorageFailed),
                    // Create a new round instance and save it to storage.
                    _ => Round::new(
                        &self.environment,
                        round_height,
                        started_at,
                        contributors,
                        verifiers,
                        chunk_verifiers,
                        chunk_verified_locators,
                    )?,
                }
            };

            debug!("Starting initialization of round {}", round_height);

            // Execute initialization of contribution 0 for all chunks in the
            // new round and check that the new locators exist.
            for chunk_id in 0..self.environment.number_of_chunks() {
                info!("Coordinator is starting initialization on chunk {}", chunk_id);
                // TODO (howardwu): Add contribution hash to `Round`.
                let _contribution_hash = Initialization::run(&self.environment, round_height, chunk_id)?;
                info!("Coordinator completed initialization on chunk {}", chunk_id);

                // Check that the contribution locator corresponding to this round's chunk now exists.
                let contribution_locator = self.environment.contribution_locator(round_height, chunk_id, 0);
                if !Path::new(&contribution_locator).exists() {
                    return Err(CoordinatorError::RoundTranscriptMissing);
                }

                // Check that the contribution locator corresponding to the next round's chunk now exists.
                let contribution_locator = self.environment.contribution_locator(round_height + 1, chunk_id, 0);
                if !Path::new(&contribution_locator).exists() {
                    return Err(CoordinatorError::RoundTranscriptMissing);
                }

                // Attempt to acquire the lock for verification.
                // self.try_lock_for_verification(chunk_id, 0)?;

                // Runs verification and on success, updates the chunk contribution to verified.
                // self.verify_contribution(chunk_id, 0)?;
            }

            // Write the round to storage.
            self.set_round(round_height, round)?;

            // Check that the current round now matches the given round height unconditionally.
            if self.current_round_height()? != round_height {
                return Err(CoordinatorError::RoundHeightMismatch);
            }

            debug!("Completed initialization of round {}", round_height);

            // // Execute initialization of contribution 0 for all chunks in the
            // // new round and check that the new locators exist.
            // let new_height = round_height + 1;
            // debug!("Starting initialization of round {}", new_height);
            // for chunk_id in 0..self.environment.number_of_chunks() {
            //     info!("Coordinator is starting initialization on chunk {}", chunk_id);
            //     // TODO (howardwu): Add contribution hash to `Round`.
            //     let _contribution_hash = Initialization::run(&self.environment, new_height, chunk_id)?;
            //     info!("Coordinator completed initialization on chunk {}", chunk_id);
            //
            //     // Check that the contribution locator corresponding to this round and chunk now exists.
            //     let contribution_locator = self.environment.contribution_locator(new_height, chunk_id, 0);
            //     if !Path::new(&contribution_locator).exists() {
            //         return Err(CoordinatorError::RoundTranscriptMissing);
            //     }
            //
            //     // Attempt to acquire the lock for verification.
            //     // self.try_lock_for_verification(chunk_id, 0)?;
            //
            //     // Runs verification and on success, updates the chunk contribution to verified.
            //     // self.verify_contribution(chunk_id, 0)?;
            // }
        }

        // Create the new round height.
        let new_height = round_height + 1;

        debug!("Starting initialization of round {}", new_height);

        // Check that the new round does not exist in storage.
        // If it exists, this means the round was already initialized.
        match self.storage()?.get(&Key::Round(new_height)) {
            Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
            Some(_) => return Err(CoordinatorError::StorageFailed),
            _ => (),
        };

        // Check that each contribution transcript for the next round exists.
        for chunk_id in 0..self.environment.number_of_chunks() {
            debug!("Locating round {} chunk {} contribution 0", new_height, chunk_id);
            let contribution_locator = self.environment.contribution_locator(new_height, chunk_id, 0);
            if !Path::new(&contribution_locator).exists() {
                return Err(CoordinatorError::RoundTranscriptMissing);
            }
        }

        // Instantiate the new round and height.
        let new_round = Round::new(
            &self.environment,
            new_height,
            started_at,
            contributors,
            verifiers,
            chunk_verifiers,
            chunk_verified_base_url,
        )?;

        // Insert and save the new round into storage.
        self.set_round(new_height, new_round);

        debug!("Completed initialization of round {}", new_height);
        Ok(())
    }

    /// Attempts to acquire the lock for a given chunk ID and contribution ID
    /// in order to perform verification.
    #[inline]
    fn try_lock_verify(&self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Fetch the current height from storage.
        let round_height = self.current_round_height()?;

        // TODO (howardwu): Remove this restriction on round height == 0.
        // Check that the contribution locator corresponding to this round and chunk exists.
        if round_height == 0 {
            let contribution_locator = self
                .environment
                .contribution_locator(round_height, chunk_id, contribution_id);
            if !Path::new(&contribution_locator).exists() {
                return Err(CoordinatorError::RoundTranscriptMissing);
            }
        }

        // Load a mutable reference of the current round from storage.
        let mut storage = self.storage_mut()?;
        let mut current_round = match storage.get_mut(&Key::Round(round_height)) {
            Some(Value::Round(round)) => round,
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the height set in `round` matches the current round height.
        if current_round.get_height() != round_height {
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // Fetch the verifier of the coordinator and attempt to
        // acquire the chunk lock for verification.
        let verifier = self.environment.coordinator_verifier();
        if !verifier.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        current_round.try_lock_chunk(chunk_id, verifier.clone())?;
        info!(
            "Coordinator verifier acquired lock on round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );
        Ok(())
    }

    /// Attempts to run verification in the current round for a given chunk ID and contribution ID.
    /// On success, this function copies the current contribution into the next transcript locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    #[inline]
    fn verify_contribution(&self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Fetch the current height from storage.
        let round_height = self.current_round_height()?;

        // Fetch the verifier of the coordinator and
        // check that the chunk lock is currently held by this verifier.
        let verifier = self.environment.coordinator_verifier();
        if !self.get_round(round_height)?.is_chunk_locked_by(chunk_id, &verifier) {
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the contribution locator corresponding to this round and chunk exists.
        let contribution_locator = self
            .environment
            .contribution_locator(round_height, chunk_id, contribution_id);
        if !Path::new(&contribution_locator).exists() {
            return Err(CoordinatorError::RoundTranscriptMissing);
        }

        // Fetch the number of contributors, which is used by `Verification` to determine
        // whether the next transcript is a new contribution ID or a new round ID.
        let num_contributors = self.current_round()?.num_contributors();

        debug!("Coordinator is starting verification on chunk {}", chunk_id);
        Verification::run(
            &self.environment,
            round_height,
            chunk_id,
            contribution_id,
            num_contributors,
        )?;
        debug!("Coordinator completed verification on chunk {}", chunk_id);

        // Load a mutable reference of the current round from storage.
        let mut storage = self.storage_mut()?;
        let mut current_round = match storage.get_mut(&Key::Round(round_height)) {
            Some(Value::Round(round)) => round,
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the height set in `round` matches the current round height.
        if current_round.get_height() != round_height {
            return Err(CoordinatorError::RoundHeightMismatch);
        }

        // Attempts to set the current contribution as verified in the current round.
        current_round.verify_contribution(chunk_id, contribution_id, &verifier)?;
        info!("Verified chunk {} contribution {}", chunk_id, contribution_id);
        Ok(())
    }

    /// Attempts to run aggregation for the current round.
    #[inline]
    fn aggregate_current_round(&self) -> Result<(), CoordinatorError> {
        // Fetch the current round.
        let current_round = self.current_round()?;
        let current_round_height = current_round.get_height();

        // Check that all current round chunks are fully contributed and verified.
        if !current_round.is_complete() {
            return Err(CoordinatorError::RoundNotComplete);
        }

        // TODO (howardwu): Do pre-check that all current chunk contributions are present
        // Check that the transcript directory corresponding to this round exists.
        let round_directory = self.environment.round_directory(current_round_height);
        let path = Path::new(&round_directory);
        if !path.exists() {
            return Err(CoordinatorError::RoundTranscriptMissing);
        }

        // TODO (howardwu): Add aggregate verification logic.
        // Execute aggregation to combine on all chunks to finalize the round
        // corresponding to the given round height.
        debug!("Coordinator is starting aggregation");
        Aggregation::run(&self.environment, &current_round)?;
        debug!("Coordinator completed aggregation");

        // Fetch the final round transcript locator for the given round.
        let round_locator = self.environment.final_round_locator(current_round_height);

        // Check that the final round transcript locator exists.
        if !Path::new(&round_locator).exists() {
            return Err(CoordinatorError::FinalRoundTranscriptMissing);
        }

        Ok(())
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
