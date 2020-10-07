use crate::{
    commands::{Aggregation, Computation, Initialization, Verification},
    environment::Environment,
    objects::{Participant, Round},
    storage::{Key, Storage, Value},
};

use chrono::{DateTime, Utc};
use std::{
    fmt,
    sync::{Arc, RwLock},
};
use tracing::{debug, error, info, trace};

#[derive(Debug)]
pub enum CoordinatorError {
    ChunkAlreadyComplete,
    ChunkAlreadyVerified,
    ChunkIdInvalid,
    ChunkIdMismatch,
    ChunkLockAlreadyAcquired,
    ChunkLockLimitReached,
    ChunkMissing,
    ChunkMissingVerification,
    ChunkNotLockedOrByWrongParticipant,
    ComputationFailed,
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFileSizeMismatch,
    ContributionIdIsNonzero,
    ContributionIdMismatch,
    ContributionIdMustBeNonzero,
    ContributionLocatorAlreadyExists,
    ContributionLocatorMissing,
    ContributionMissing,
    ContributionMissingVerification,
    ContributionMissingVerifiedLocator,
    ContributionMissingVerifier,
    ContributionShouldNotExist,
    ContributionsComplete,
    ContributorAlreadyContributed,
    ContributorsMissing,
    ExpectedContributor,
    ExpectedVerifier,
    Error(anyhow::Error),
    InitializationFailed,
    InitializationTranscriptsDiffer,
    IOError(std::io::Error),
    JsonError(serde_json::Error),
    Launch(rocket::error::LaunchError),
    NumberOfChunksInvalid,
    NumberOfContributionsDiffer,
    RoundAggregationFailed,
    RoundAlreadyInitialized,
    RoundContributorsMissing,
    RoundContributorsNotUnique,
    RoundDirectoryMissing,
    RoundDoesNotExist,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundLocatorAlreadyExists,
    RoundLocatorMissing,
    RoundNotComplete,
    RoundShouldNotExist,
    RoundVerifiersMissing,
    RoundVerifiersNotUnique,
    StorageFailed,
    StorageUpdateFailed,
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    Url(url::ParseError),
    VerificationFailed,
    VerificationOnContributionIdZero,
    VerifierMissing,
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(error: anyhow::Error) -> Self {
        CoordinatorError::Error(error)
    }
}

impl From<serde_json::Error> for CoordinatorError {
    fn from(error: serde_json::Error) -> Self {
        CoordinatorError::JsonError(error)
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
    storage: Arc<RwLock<Box<dyn Storage>>>,
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
            storage: Arc::new(RwLock::new(environment.storage()?)),
            environment,
        })
    }

    ///
    /// Returns a reference to the round corresponding to the given height from storage.
    ///
    /// If there are no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn get_round(&self, round_height: u64) -> Result<Round, CoordinatorError> {
        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Fetch the round corresponding to the given round height from storage.
        match storage.get(&Key::RoundHeight) {
            // Check that the given round height is valid
            Some(Value::RoundHeight(current_round_height)) => match round_height <= current_round_height {
                // Load the corresponding round data from storage.
                true => match storage.get(&Key::Round(round_height)) {
                    Some(Value::Round(round)) => Ok(round),
                    _ => Err(CoordinatorError::StorageFailed),
                },
                // The given round height does not exist.
                false => Err(CoordinatorError::RoundDoesNotExist),
            },
            // There are no prior rounds of the ceremony.
            _ => Err(CoordinatorError::RoundDoesNotExist),
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
        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Fetch the current round from storage.
        match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => match round_height != 0 {
                // Case 1 - This is a typical round of the ceremony.
                // Load the corresponding round data from storage.
                true => match storage.get(&Key::Round(round_height)) {
                    Some(Value::Round(round)) => Ok(round),
                    _ => return Err(CoordinatorError::StorageFailed),
                },
                // Case 2 - There are no prior rounds of the ceremony.
                false => return Err(CoordinatorError::RoundDoesNotExist),
            },
            // Case 2 - There are no prior rounds of the ceremony.
            _ => return Err(CoordinatorError::RoundDoesNotExist),
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
        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Fetch the current round height from storage.
        match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => match round_height != 0 {
                // Case 1 - This is a typical round of the ceremony.
                // Check that the corresponding round data exists in storage.
                true => match storage.contains_key(&Key::Round(round_height)) {
                    true => Ok(round_height),
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
    /// Attempts to acquire the lock of a given chunk ID from storage
    /// for a given participant.
    ///
    /// On success, this function returns the next contribution locator
    /// if the participant is a contributor, and it returns the current
    /// contribution locator if the participant is a verifier.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_lock_chunk(&self, chunk_id: u64, participant: &Participant) -> Result<String, CoordinatorError> {
        info!("Trying to lock chunk {} for {}", chunk_id, participant);

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Acquire the storage lock.
        let mut storage = self.storage.write().unwrap();

        // Fetch the current round height from storage.
        let round_height = match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => round_height,
            // Case 2 - There are no prior rounds of the ceremony.
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        trace!("Current round height from storage is {}", round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match round_height != 0 {
            // Case 1 - This is a typical round of the ceremony.
            // Load the corresponding round data from storage.
            true => match storage.get(&Key::Round(round_height)) {
                Some(Value::Round(round)) => round,
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 2 - There are no prior rounds of the ceremony.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Attempt to acquire the chunk lock for participant.
        let contribution_locator = round.try_lock_chunk(&self.environment, chunk_id, &participant)?;

        // Add the updated round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round)) {
            // Next, save the round to storage.
            if storage.save() {
                debug!("Updated round {} in storage", round_height);
                info!("{} acquired lock on chunk {}", participant, chunk_id);
                return Ok(contribution_locator);
            }
        }

        Err(CoordinatorError::StorageUpdateFailed)
    }

    ///
    /// Attempts to add a contribution for a given chunk ID from a given participant.
    ///
    /// On success, this function releases the lock from the contributor and returns
    /// the next contribution locator.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn add_contribution(&self, chunk_id: u64, participant: &Participant) -> Result<String, CoordinatorError> {
        info!("Adding contribution from {} to chunk {}", participant, chunk_id);

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Acquire the storage lock.
        let mut storage = self.storage.write().unwrap();

        // Fetch the current round height from storage.
        let round_height = match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => round_height,
            // Case 2 - There are no prior rounds of the ceremony.
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        trace!("Current round height from storage is {}", round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match round_height != 0 {
            // Case 1 - This is a typical round of the ceremony.
            // Load the corresponding round data from storage.
            true => match storage.get(&Key::Round(round_height)) {
                Some(Value::Round(round)) => round,
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 2 - There are no prior rounds of the ceremony.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the participant is an authorized contributor to the current round.
        if !round.is_authorized_contributor(participant) {
            error!("{} is unauthorized to contribute to chunk {})", participant, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, participant) {
            error!("{} should have lock on chunk {} but does not", participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = round.expected_number_of_contributions();

        // Fetch the next contribution ID of the chunk.
        let next_contribution_id = round
            .get_chunk(chunk_id)?
            .next_contribution_id(expected_num_contributions)?;

        // Fetch the contribution locator for the next contribution ID corresponding to
        // the current round height and chunk ID.
        let next_contribution_locator =
            self.environment
                .contribution_locator(round_height, chunk_id, next_contribution_id, false);

        // Add the next contribution to the current chunk.
        round.get_chunk_mut(chunk_id)?.add_contribution(
            next_contribution_id,
            participant,
            next_contribution_locator.clone(),
            expected_num_contributions,
        )?;

        trace!("Next contribution locator is {}", next_contribution_locator);
        {
            // TODO (howardwu): Check that the file size is nonzero, the structure is correct,
            //  and the starting hash is based on the previous contribution.
        }
        // Add the updated round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round)) {
            // Next, save the round to storage.
            if storage.save() {
                debug!("Updated round {} in storage", round_height);
                {
                    // TODO (howardwu): Send job to run verification on new chunk.
                }
                info!("{} added a contribution to chunk {}", participant, chunk_id);
                return Ok(next_contribution_locator);
            }
        }

        Err(CoordinatorError::StorageUpdateFailed)
    }

    ///
    /// Attempts to run verification in the current round for a given chunk ID and participant.
    ///
    /// On success, this function copies the current contribution into the next transcript locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    #[inline]
    pub fn verify_contribution(&self, chunk_id: u64, participant: &Participant) -> Result<(), CoordinatorError> {
        info!("Attempting to verify a contribution for chunk {}", chunk_id);

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Acquire the storage lock.
        let mut storage = self.storage.write().unwrap();

        // Fetch the current round height from storage.
        let round_height = match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => round_height,
            // Case 2 - There are no prior rounds of the ceremony.
            _ => return Err(CoordinatorError::RoundDoesNotExist),
        };

        trace!("Current round height from storage is {}", round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match round_height != 0 {
            // Case 1 - This is a typical round of the ceremony.
            // Load the corresponding round data from storage.
            true => match storage.get(&Key::Round(round_height)) {
                Some(Value::Round(round)) => round,
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 2 - There are no prior rounds of the ceremony.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the participant is an authorized verifier to the current round.
        if !round.is_authorized_verifier(participant) {
            error!("{} is unauthorized to verify chunk {})", participant, chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Check that the chunk lock is currently held by this verifier.
        if !round.is_chunk_locked_by(chunk_id, participant) {
            error!("{} should have lock on chunk {} but does not", participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.get_chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();
        // Fetch the next contribution ID.
        let current_contribution = chunk.current_contribution()?;

        // Check that the contribution locator corresponding to this round and chunk exists.
        if !self
            .environment
            .contribution_locator_exists(round_height, chunk_id, current_contribution_id, false)
        {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Check if the current contribution has already been verified.
        if current_contribution.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch the contribution locators for `Verification`.
        let (previous, current, next) = if chunk.only_contributions_complete(round.expected_number_of_contributions()) {
            let previous =
                self.environment
                    .contribution_locator(round_height, chunk_id, current_contribution_id - 1, true);
            let current = self
                .environment
                .contribution_locator(round_height, chunk_id, current_contribution_id, false);
            let next = self
                .environment
                .contribution_locator(round_height + 1, chunk_id, 0, true);

            // Initialize the chunk directory of the new round so the next locator file will be saved.
            self.environment.chunk_directory_init(round_height + 1, chunk_id);

            (previous, current, next)
        } else {
            let previous =
                self.environment
                    .contribution_locator(round_height, chunk_id, current_contribution_id - 1, true);
            let current = self
                .environment
                .contribution_locator(round_height, chunk_id, current_contribution_id, false);
            let next = self
                .environment
                .contribution_locator(round_height, chunk_id, current_contribution_id, true);
            (previous, current, next)
        };

        debug!("Coordinator is starting verification on chunk {}", chunk_id);
        Verification::run(
            &self.environment,
            round_height,
            chunk_id,
            current_contribution_id,
            previous,
            current.clone(),
            next,
        )?;
        debug!("Coordinator completed verification on chunk {}", chunk_id);

        // Attempts to set the current contribution as verified in the current round.
        round.verify_contribution(chunk_id, current_contribution_id, participant.clone(), current)?;

        // Add the updated round to storage.
        if storage.insert(Key::Round(round_height), Value::Round(round)) {
            // Next, save the round to storage.
            if storage.save() {
                debug!("Updated round {} in storage", round_height);
                info!(
                    "{} verified chunk {} contribution {}",
                    participant, chunk_id, current_contribution_id
                );
                return Ok(());
            }
        }

        Err(CoordinatorError::StorageUpdateFailed)
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
        &self,
        started_at: DateTime<Utc>,
        contributors: Vec<Participant>,
        verifiers: Vec<Participant>,
    ) -> Result<u64, CoordinatorError> {
        // Check that the next round has at least one authorized contributor.
        if contributors.is_empty() {
            return Err(CoordinatorError::ContributorsMissing);
        }
        // Check that the next round has at least one authorized verifier.
        if verifiers.is_empty() {
            return Err(CoordinatorError::VerifierMissing);
        }

        // Acquire the storage lock.
        let mut storage = self.storage.write().unwrap();

        // Fetch the current round height from storage.
        let round_height = match storage.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round_height)) => round_height,
            // Case 2 - There are no prior rounds of the ceremony.
            _ => 0,
        };

        trace!("Current round height from storage is {}", round_height);

        // Check that the round height corresponds to round data in storage.
        {
            if round_height == 0 {
                // Check that the current round does not exist in storage.
                if storage.get(&Key::Round(round_height)).is_some() {
                    return Err(CoordinatorError::RoundShouldNotExist);
                }
            } else {
                // Check that the current round exists in storage.
                if storage.get(&Key::Round(round_height)).is_none() {
                    return Err(CoordinatorError::RoundShouldNotExist);
                }
            }

            // Check that the next round does not exist in storage.
            if storage.get(&Key::Round(round_height + 1)).is_some() {
                return Err(CoordinatorError::RoundShouldNotExist);
            }
        }

        // If this is the initial round, ensure the round does not exist yet.
        // Attempt to load the round corresponding to the given round height from storage.
        // If there is no round in storage, proceed to create a new round instance,
        // and run `Initialization` to start the ceremony.
        if round_height == 0 {
            // If the path exists, this means a prior *ceremony* is stored as a transcript.
            //
            // In a test environment, this step clears the transcript of the coordinator.
            // In a development or production environment, this step does NOT reset
            // the transcript of the coordinator.
            if self.environment.round_directory_exists(round_height) {
                self.environment.round_directory_reset(round_height);
            }

            // Create an instantiation of `Round` for round 0.
            let round = {
                // Initialize the contributors as an empty list as this is for initialization.
                let contributors = vec![];

                // Initialize the verifiers as a list comprising only the coordinator verifier,
                // as this is for initialization.
                let verifiers = vec![self.environment.coordinator_verifier()];

                match storage.get(&Key::Round(round_height)) {
                    // Check that the round does not exist in storage.
                    // If it exists, this means the round was already initialized.
                    Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
                    Some(_) => return Err(CoordinatorError::StorageFailed),
                    // Create a new round instance and save it to storage.
                    _ => Round::new(&self.environment, round_height, started_at, contributors, verifiers)?,
                }
            };

            debug!("Starting initialization of round {}", round_height);

            // Execute initialization of contribution 0 for all chunks
            // in the new round and check that the new locators exist.
            for chunk_id in 0..self.environment.number_of_chunks() {
                // 1 - Check that the contribution locator corresponding to this round's chunk does not exist.
                if self
                    .environment
                    .contribution_locator_exists(round_height, chunk_id, 0, true)
                {
                    return Err(CoordinatorError::ContributionLocatorAlreadyExists);
                }

                // 2 - Check that the contribution locator corresponding to the next round's chunk does not exists.
                if self
                    .environment
                    .contribution_locator_exists(round_height + 1, chunk_id, 0, true)
                {
                    return Err(CoordinatorError::ContributionLocatorAlreadyExists);
                }

                info!("Coordinator is starting initialization on chunk {}", chunk_id);
                // TODO (howardwu): Add contribution hash to `Round`.
                let _contribution_hash = Initialization::run(&self.environment, round_height, chunk_id)?;
                info!("Coordinator completed initialization on chunk {}", chunk_id);

                // 1 - Check that the contribution locator corresponding to this round's chunk now exists.
                if !self
                    .environment
                    .contribution_locator_exists(round_height, chunk_id, 0, true)
                {
                    return Err(CoordinatorError::ContributionLocatorMissing);
                }

                // 2 - Check that the contribution locator corresponding to the next round's chunk now exists.
                if !self
                    .environment
                    .contribution_locator_exists(round_height + 1, chunk_id, 0, true)
                {
                    return Err(CoordinatorError::ContributionLocatorMissing);
                }
            }

            // Add the new round to storage.
            if storage.insert(Key::Round(round_height), Value::Round(round)) {
                // Next, save the round to storage.
                if storage.save() {
                    debug!("Updated round {} in storage", round_height);
                    debug!("Completed initialization of round {}", round_height);
                }
            }
        }

        // Execute aggregation of the current round in preparation for
        // transitioning to the next round. If this is the initial round,
        // there should be nothing to aggregate and we may continue.
        if round_height != 0 {
            // TODO (howardwu): Check that all locks have been released.

            // Load the current round from storage.
            let round = match storage.get(&Key::Round(round_height)) {
                Some(Value::Round(round)) => round,
                _ => return Err(CoordinatorError::StorageFailed),
            };

            // Check that all chunks in the current round are verified,
            // so that we may transition to the next round.
            if !&round.is_complete() {
                error!("Round {} is not complete and next round is not starting", round_height);
                trace!("{:#?}", &round);
                return Err(CoordinatorError::RoundNotComplete);
            }

            // Execute round aggregation and aggregate verification on the current round.
            {
                // TODO (howardwu): Do pre-check that all current chunk contributions are present.
                // Check that the round directory corresponding to this round exists.
                if !self.environment.round_directory_exists(round_height) {
                    return Err(CoordinatorError::RoundDirectoryMissing);
                }

                // Check that the round locator does not exist.
                if self.environment.round_locator_exists(round_height) {
                    return Err(CoordinatorError::RoundLocatorAlreadyExists);
                }

                // TODO (howardwu): Add aggregate verification logic.
                // Execute aggregation to combine on all chunks to finalize the round
                // corresponding to the given round height.
                debug!("Coordinator is starting aggregation");
                Aggregation::run(&self.environment, &round)?;
                debug!("Coordinator completed aggregation");

                // Check that the round locator now exists.
                if !self.environment.round_locator_exists(round_height) {
                    return Err(CoordinatorError::RoundLocatorMissing);
                }
            }
        }

        // Create the new round height.
        let new_height = round_height + 1;

        info!("Starting transition from round {} to {}", round_height, new_height);

        // Check that the new round does not exist in storage.
        // If it exists, this means the round was already initialized.
        match storage.get(&Key::Round(new_height)) {
            Some(Value::Round(_)) => return Err(CoordinatorError::RoundAlreadyInitialized),
            Some(_) => return Err(CoordinatorError::StorageFailed),
            _ => (),
        };

        // Check that each contribution for the next round exists.
        for chunk_id in 0..self.environment.number_of_chunks() {
            debug!("Locating round {} chunk {} contribution 0", new_height, chunk_id);
            if !self
                .environment
                .contribution_locator_exists(new_height, chunk_id, 0, true)
            {
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Instantiate the new round and height.
        let new_round = Round::new(&self.environment, new_height, started_at, contributors, verifiers)?;

        #[cfg(test)]
        trace!("{:#?}", &new_round);

        // Insert and save the new round into storage.
        if storage.insert(Key::Round(new_height), Value::Round(new_round)) {
            // Next, update the round height to reflect the new round.
            if storage.insert(Key::RoundHeight, Value::RoundHeight(new_height)) {
                // Lastly, save the round to storage.
                if storage.save() {
                    debug!("Added round {} to storage", round_height);
                    info!("Completed transition from round {} to {}", round_height, new_height);
                    return Ok(new_height);
                }
            }
        }

        Err(CoordinatorError::StorageUpdateFailed)
    }

    ///
    /// Returns a reference to the instantiation of `Environment` that this
    /// coordinator is using.
    ///
    #[inline]
    pub fn environment(&self) -> &Environment {
        &self.environment
    }

    ///
    /// Attempts to run computation for a given round height, given chunk ID, and contribution ID.
    ///
    /// This function is primarily used for testing purposes. This can also be purposed for
    /// completing contributions of participants who may have dropped off and handed over
    /// control of their session.
    ///
    #[cfg(test)]
    #[inline]
    fn run_computation(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
    ) -> Result<(), CoordinatorError> {
        info!(
            "Running computation for round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the contribution ID is valid.
        if contribution_id == 0 {
            return Err(CoordinatorError::ContributionIdMustBeNonzero);
        }

        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Check that the ceremony is running and fetch the specified round from storage.
        let round = match round_height != 0 {
            // Case 1 - This is a typical round of the ceremony.
            // Load the corresponding round data from storage.
            true => match storage.get(&Key::Round(round_height)) {
                Some(Value::Round(round)) => round,
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 2 - This round does not need a contributor.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, &participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the contribution locator corresponding to this round and chunk exists.
        if self
            .environment
            .contribution_locator_exists(round_height, chunk_id, contribution_id, false)
        {
            error!("Locator for contribution {} already exists", contribution_id);
            return Err(CoordinatorError::ContributionLocatorAlreadyExists);
        }

        // Fetch the current round and given chunk ID and check that
        // the given contribution ID has not been verified yet.
        let chunk = round.get_chunk(chunk_id)?;
        if chunk.get_contribution(contribution_id).is_ok() {
            return Err(CoordinatorError::ContributionShouldNotExist);
        }

        trace!(
            "Contributor starting computation on round {} chunk {} contribution {}",
            round_height,
            chunk_id,
            contribution_id
        );
        Computation::run(&self.environment, round_height, chunk_id, contribution_id)?;
        trace!(
            "Contributor completed computation on round {} chunk {} contribution {}",
            round_height,
            chunk_id,
            contribution_id
        );

        info!("Computed chunk {} contribution {}", chunk_id, contribution_id);
        Ok(())
    }

    // /// Attempts to run verification in the current round for a given chunk ID.
    // #[inline]
    // fn verify_chunk(&self, chunk_id: u64) -> Result<(), CoordinatorError> {
    //     // Fetch the current round.
    //     let mut current_round = self.current_round()?;
    //     let round_height = current_round.round_height();
    //
    //     // Execute verification of contribution ID for all chunks in the
    //     // new round and check that the new locators exist.
    //     let new_height = round_height + 1;
    //     debug!("Starting verification of round {}", new_height);
    //     for chunk_id in 0..self.environment.number_of_chunks() {
    //     info!("Coordinator is starting initialization on chunk {}", chunk_id);
    //     // TODO (howardwu): Add contribution hash to `Round`.
    //     let _contribution_hash = Initialization::run(&self.environment, new_height, chunk_id)?;
    //     info!("Coordinator completed initialization on chunk {}", chunk_id);
    //
    //     // Check that the contribution locator corresponding to this round and chunk now exists.
    //     let contribution_locator = self.environment.contribution_locator(new_height, chunk_id, contribution_id);
    //     if !Path::new(&contribution_locator).exists() {
    //         return Err(CoordinatorError::RoundTranscriptMissing);
    //     }
    //
    //     // Attempt to acquire the lock for verification.
    //     // self.try_lock(chunk_id, contribution_id)?;
    //
    //     // Runs verification and on success, updates the chunk contribution to verified.
    //     // self.verify_contribution(chunk_id, contribution_id)?;
    // }
}

#[cfg(test)]
mod test {
    use crate::{environment::*, testing::prelude::*, Coordinator};

    use chrono::Utc;
    use once_cell::sync::Lazy;
    use std::{panic, process};

    fn initialize_coordinator(coordinator: &Coordinator) -> anyhow::Result<()> {
        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height()?);

        // Run initialization.
        coordinator.next_round(
            *TEST_STARTED_AT,
            vec![
                Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
                Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
            ],
            vec![Lazy::force(&TEST_VERIFIER_ID).clone()],
        )?;

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height()?);

        std::thread::sleep(std::time::Duration::from_secs(1));
        Ok(())
    }

    fn initialize_coordinator_single_contributor(coordinator: &Coordinator) -> anyhow::Result<()> {
        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height()?);

        // Run initialization.
        coordinator.next_round(*TEST_STARTED_AT, vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()], vec![
            Lazy::force(&TEST_VERIFIER_ID).clone(),
        ])?;

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height()?);
        Ok(())
    }

    fn coordinator_initialization_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone())?;

        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height()?);

        // Run initialization.
        coordinator.next_round(
            Utc::now(),
            vec![
                Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
                Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
            ],
            vec![Lazy::force(&TEST_VERIFIER_ID).clone()],
        )?;

        {
            // Check round 0 is complete.
            assert!(coordinator.get_round(0)?.is_complete());

            // Check current round height is now 1.
            assert_eq!(1, coordinator.current_round_height()?);

            // Check the current round has a matching height.
            let current_round = coordinator.current_round()?;
            assert_eq!(coordinator.current_round_height()?, current_round.round_height());

            // Check round 1 contributors.
            assert_eq!(2, current_round.number_of_contributors());
            assert!(current_round.is_authorized_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(current_round.is_authorized_contributor(&TEST_CONTRIBUTOR_ID_2));
            assert!(!current_round.is_authorized_contributor(&TEST_CONTRIBUTOR_ID_3));
            assert!(!current_round.is_authorized_contributor(&TEST_VERIFIER_ID));

            // Check round 1 verifiers.
            assert_eq!(1, current_round.number_of_verifiers());
            assert!(current_round.is_authorized_verifier(&TEST_VERIFIER_ID));
            assert!(!current_round.is_authorized_verifier(&TEST_VERIFIER_ID_2));
            assert!(!current_round.is_authorized_verifier(&TEST_CONTRIBUTOR_ID));

            // Check round 1 is NOT complete.
            assert!(!current_round.is_complete());
        }

        Ok(())
    }

    fn coordinator_contributor_try_lock_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone())?;
        initialize_coordinator(&coordinator)?;

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        let contributor_2 = Lazy::force(&TEST_CONTRIBUTOR_ID_2);

        {
            // Acquire the lock for chunk 0 as contributor 1.
            assert!(coordinator.try_lock_chunk(0, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 1 again.
            assert!(coordinator.try_lock_chunk(0, &contributor).is_err());

            // Acquire the lock for chunk 1 as contributor 1.
            assert!(coordinator.try_lock_chunk(1, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 2.
            assert!(coordinator.try_lock_chunk(0, &contributor_2).is_err());

            // Attempt to acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(1, &contributor_2).is_err());

            // Acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(2, &contributor_2).is_ok());
        }

        {
            // Check that chunk 0 is locked.
            let round = coordinator.current_round()?;
            let chunk = round.get_chunk(0)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 1 is locked.
            let chunk = round.get_chunk(1)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 1 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 2 is locked.
            let chunk = round.get_chunk(2)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 2 is locked by contributor 2.
            assert!(chunk.is_locked_by(contributor_2));
            assert!(!chunk.is_locked_by(contributor));
        }

        Ok(())
    }

    fn coordinator_contributor_add_contribution_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone())?;
        initialize_coordinator(&coordinator)?;

        // Acquire the lock for chunk 0 as contributor 1.
        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        coordinator.try_lock_chunk(0, &contributor)?;

        // Run computation on round 1 chunk 0 contribution 1.
        {
            // Check current round is 1.
            let round = coordinator.current_round()?;
            let round_height = round.round_height();
            assert_eq!(1, round_height);

            // Check chunk 0 is not verified.
            let chunk_id = 0;
            let chunk = round.get_chunk(chunk_id)?;
            assert!(!chunk.is_complete(round.expected_number_of_contributions()));

            // Check next contribution is 1.
            let contribution_id = 1;
            assert!(chunk.is_next_contribution_id(contribution_id, round.expected_number_of_contributions()));

            // Run the computation
            assert!(
                coordinator
                    .run_computation(round_height, chunk_id, contribution_id, &contributor)
                    .is_ok()
            );
        }

        // Add contribution for round 1 chunk 0 contribution 1.
        {
            // Add round 1 chunk 0 contribution 1.
            let chunk_id = 0;
            assert!(coordinator.add_contribution(chunk_id, &contributor).is_ok());

            // Check chunk 0 lock is released.
            let round = coordinator.current_round()?;
            let chunk = round.get_chunk(chunk_id)?;
            assert!(chunk.is_unlocked());
            assert!(!chunk.is_locked());
        }

        Ok(())
    }

    fn coordinator_verifier_verify_contribution_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone())?;
        initialize_coordinator(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Acquire the lock for chunk 0 as contributor 1.
        let chunk_id = 0;
        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        assert!(coordinator.try_lock_chunk(chunk_id, &contributor).is_ok());

        // Run computation on round 1 chunk 0 contribution 1.
        let contribution_id = 1;
        assert!(
            coordinator
                .run_computation(round_height, chunk_id, contribution_id, contributor)
                .is_ok()
        );

        // Add round 1 chunk 0 contribution 1.
        assert!(coordinator.add_contribution(chunk_id, &contributor).is_ok());

        // Acquire lock for round 1 chunk 0 contribution 1.
        {
            // Acquire the lock on chunk 0 for the verifier.
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
            assert!(coordinator.try_lock_chunk(chunk_id, &verifier).is_ok());

            // Check that chunk 0 is locked.
            let round = coordinator.current_round()?;
            let chunk = round.get_chunk(chunk_id)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by the verifier.
            assert!(chunk.is_locked_by(&verifier));
        }

        // Verify round 1 chunk 0 contribution 1.
        {
            // Verify contribution 1.
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
            coordinator.verify_contribution(chunk_id, &verifier)?;
        }

        Ok(())
    }

    // This test runs a round with a single coordinator and single verifier
    // The verifier instances are run on a separate thread to simulate an environment where
    // verification and contribution happen concurrently.
    fn coordinator_concurrent_contribution_verification_test() -> anyhow::Result<()> {
        clear_test_transcript();

        // take_hook() returns the default hook in case when a custom one is not set
        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone())?;
        initialize_coordinator_single_contributor(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        let coordinator = std::sync::Arc::new(coordinator);
        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let verifier = Lazy::force(&TEST_VERIFIER_ID);

        let mut verifier_threads = vec![];

        let contribution_id = 1;
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            {
                // Acquire the lock as contributor.
                let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    try_lock?;
                }

                // Run computation as contributor.
                let contribute = coordinator.run_computation(round_height, chunk_id, contribution_id, &contributor);
                if contribute.is_err() {
                    println!(
                        "Failed to run computation for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    contribute?;
                }

                // Add the contribution as the contributor.
                let contribute = coordinator.add_contribution(chunk_id, &contributor);
                if contribute.is_err() {
                    println!(
                        "Failed to add contribution for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round()?)?
                    );
                    contribute?;
                }
            }

            // Spawn a thread to concurrently verify the contributions.
            let coordinator_clone = coordinator.clone();
            let verifier_thread = std::thread::spawn(move || {
                let verifier = verifier.clone();

                // Acquire the lock as the verifier.
                let try_lock = coordinator_clone.try_lock_chunk(chunk_id, &verifier);
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock as verifier {:?}\n{}",
                        verifier.clone(),
                        serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                    );
                    panic!(format!("{:?}", try_lock.unwrap()))
                }

                // Run verification as the verifier.
                let verify = coordinator_clone.verify_contribution(chunk_id, &verifier);
                if verify.is_err() {
                    println!(
                        "Failed to run verification as verifier {:?}\n{}",
                        verifier.clone(),
                        serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                    );
                    panic!(format!("{:?}", verify.unwrap()))
                }
            });
            verifier_threads.push(verifier_thread);
        }

        for verifier_thread in verifier_threads {
            verifier_thread.join().expect("Couldn't join on the verifier thread");
        }

        Ok(())
    }

    fn coordinator_aggregation_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone())?;
        initialize_coordinator(&coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Run computation and verification on each contribution in each chunk.
        let contributors = vec![
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
        ];

        let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();

        // Iterate over all chunk IDs.
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                let contributor = &contributors[contribution_id as usize - 1];
                {
                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                    if try_lock.is_err() {
                        println!(
                            "Failed to acquire lock as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }

                    // Run computation as contributor.
                    let contribute = coordinator.run_computation(round_height, chunk_id, contribution_id, &contributor);
                    if contribute.is_err() {
                        println!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(chunk_id, &contributor);
                    if contribute.is_err() {
                        println!(
                            "Failed to add contribution as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }
                }
                {
                    // Acquire the lock as the verifier.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &verifier);
                    if try_lock.is_err() {
                        println!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }

                    // Run verification as the verifier.
                    let verify = coordinator.verify_contribution(chunk_id, &verifier);
                    if verify.is_err() {
                        println!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }
                }
            }
        }

        println!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        // Run aggregation and transition from round 1 to round 2.
        coordinator.next_round(Utc::now(), contributors, vec![verifier.clone()])?;

        println!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    fn coordinator_next_round_test() -> anyhow::Result<()> {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone())?;
        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let verifier = Lazy::force(&TEST_VERIFIER_ID);

        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height()?);
        // Run initialization.
        coordinator.next_round(*TEST_STARTED_AT, vec![contributor.clone()], vec![verifier.clone()])?;
        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Run computation and verification on each contribution in each chunk.
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // Ensure contribution ID 0 is already verified by the coordinator.
            assert!(
                coordinator
                    .current_round()?
                    .get_chunk(chunk_id)?
                    .get_contribution(0)?
                    .is_verified()
            );

            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                {
                    // Acquire the lock as contributor.
                    if coordinator.try_lock_chunk(chunk_id, &contributor).is_err() {
                        panic!(
                            "Failed to acquire lock as contributor {}",
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                    }
                    // Run computation as contributor.
                    if coordinator
                        .run_computation(round_height, chunk_id, contribution_id, &contributor)
                        .is_err()
                    {
                        panic!(
                            "Failed to run computation as contributor {}",
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                    }
                    // Add the contribution as the contributor.
                    if coordinator.add_contribution(chunk_id, &contributor).is_err() {
                        panic!(
                            "Failed to add contribution as contributor {}",
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                    }
                }
                {
                    // Acquire the lock as the verifier.
                    if coordinator.try_lock_chunk(chunk_id, &verifier).is_err() {
                        panic!(
                            "Failed to acquire lock as verifier {}",
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                    }
                    // Run verification as the verifier.
                    if coordinator.verify_contribution(chunk_id, &verifier).is_err() {
                        panic!(
                            "Failed to run verification as verifier {}",
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                    }
                }
            }
        }

        println!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        // Run aggregation and transition from round 1 to round 2.
        coordinator.next_round(Utc::now(), vec![contributor.clone()], vec![verifier.clone()])?;

        println!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn test_coordinator_initialization_matches_json() {
        clear_test_transcript();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone()).unwrap();
        initialize_coordinator(&coordinator).unwrap();

        // Check that round 0 matches the round 0 JSON specification.
        {
            // Fetch round 0 from coordinator.
            let expected = test_round_0_json().unwrap();
            let candidate = coordinator.get_round(0).unwrap();
            print_diff(&expected, &candidate);
            assert_eq!(expected, candidate);
        }
    }

    #[test]
    #[serial]
    fn test_coordinator_initialization() {
        coordinator_initialization_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_contributor_try_lock() {
        coordinator_contributor_try_lock_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_contributor_add_contribution() {
        coordinator_contributor_add_contribution_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_contributor_verify_contribution() {
        coordinator_verifier_verify_contribution_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_concurrent_contribution_verification() {
        coordinator_concurrent_contribution_verification_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_aggregation() {
        coordinator_aggregation_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_next_round() {
        coordinator_next_round_test().unwrap();
    }

    #[test]
    #[serial]
    fn test_coordinator_number_of_chunks() {
        clear_test_transcript();

        let environment = Environment::Test(Parameters::AleoTestChunks(4096));

        let coordinator = Coordinator::new(environment.clone()).unwrap();
        initialize_coordinator(&coordinator).unwrap();

        assert_eq!(
            environment.number_of_chunks(),
            coordinator.get_round(0).unwrap().get_chunks().len() as u64
        );
    }
}
