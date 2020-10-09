use crate::{
    commands::{Aggregation, Computation, Initialization, Verification},
    environment::Environment,
    objects::{Participant, Round},
    storage::{Locator, Object, Storage},
};

use chrono::{DateTime, Utc};
use serde::{de, ser};
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
    Integer(std::num::ParseIntError),
    IOError(std::io::Error),
    JsonError(serde_json::Error),
    Launch(rocket::error::LaunchError),
    LocatorDeserializationFailed,
    LocatorSerializationFailed,
    NumberOfChunksInvalid,
    NumberOfContributionsDiffer,
    Phase1Setup(setup_utils::Error),
    // PoisonedReadLock(PoisonedReadLock),
    // PoisonedWriteLock(PoisonedWriteLock),
    RoundAggregationFailed,
    RoundAlreadyInitialized,
    RoundContributorsMissing,
    RoundContributorsNotUnique,
    RoundDirectoryMissing,
    RoundDoesNotExist,
    RoundFileSizeMismatch,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundLocatorAlreadyExists,
    RoundLocatorMissing,
    RoundNotComplete,
    RoundShouldNotExist,
    RoundVerifiersMissing,
    RoundVerifiersNotUnique,
    StorageCopyFailed,
    StorageFailed,
    StorageInitializationFailed,
    StorageLocatorAlreadyExists,
    StorageLocatorFormatIncorrect,
    StorageLocatorMissing,
    StorageLockFailed,
    StorageSizeLookupFailed,
    StorageUpdateFailed,
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    Url(url::ParseError),
    VerificationFailed,
    VerificationOnContributionIdZero,
    VerifierMissing,
}

// pub type PoisonedReadLock = std::sync::PoisonError<std::sync::RwLockReadGuard<'_, std::fs::File>>;
// pub type PoisonedWriteLock = std::sync::PoisonError<std::sync::RwLockWriteGuard<'_, std::fs::File>>;

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

impl From<setup_utils::Error> for CoordinatorError {
    fn from(error: setup_utils::Error) -> Self {
        CoordinatorError::Phase1Setup(error)
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(error: std::io::Error) -> Self {
        CoordinatorError::IOError(error)
    }
}

impl From<std::num::ParseIntError> for CoordinatorError {
    fn from(error: std::num::ParseIntError) -> Self {
        CoordinatorError::Integer(error)
    }
}

// impl From<PoisonedReadLock> for CoordinatorError {
//     fn from(error: PoisonedReadLock) -> Self {
//         CoordinatorError::PoisonedReadLock(error)
//     }
// }
//
// impl From<PoisonedWriteLock> for CoordinatorError {
//     fn from(error: PoisonedWriteLock) -> Self {
//         CoordinatorError::PoisonedWriteLock(error)
//     }
// }

impl From<url::ParseError> for CoordinatorError {
    fn from(error: url::ParseError) -> Self {
        CoordinatorError::Url(error)
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        error!("{}", self);
        write!(f, "{:?}", self)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        error!("{}", error);
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

        // Fetch the current round height from storage.
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        // Check that the given round height is valid.
        match round_height != 0 && round_height <= current_round_height {
            // Fetch the round corresponding to the given round height from storage.
            true => Ok(serde_json::from_slice(
                &*storage.reader(&Locator::RoundState(round_height))?.as_ref(),
            )?),
            // The given round height does not exist.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Returns a reference to the current round of the ceremony
    /// from storage, irrespective of the stage of its completion.
    ///
    /// If there are no prior rounds in storage, returns `CoordinatorError`.
    ///
    /// When loading the current round from storage, this function
    /// checks that the current round height matches the height
    /// set in the returned `Round` instance.
    ///
    #[inline]
    pub fn current_round(&self) -> Result<Round, CoordinatorError> {
        trace!("Fetching the current round from storage");

        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Fetch the current round height from storage.
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        // Fetch the current round from storage.
        match current_round_height != 0 {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState(current_round_height))? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => Ok(round),
                // Case 2 - Storage failed to fetch the round height.
                _ => Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => Err(CoordinatorError::RoundDoesNotExist),
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
        trace!("Fetching the current round height from storage");

        // Acquire the storage lock.
        let storage = self.storage.read().unwrap();

        // Fetch the current round height from storage.
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        match current_round_height != 0 {
            // Check that the corresponding round data exists in storage.
            true => match storage.exists(&Locator::RoundState(current_round_height)) {
                // Case 1 - This is a typical round of the ceremony.
                true => Ok(current_round_height),
                // Case 2 - Storage failed to locate the current round.
                false => Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => Ok(0),
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
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        trace!("Current round height from storage is {}", current_round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match current_round_height != 0 {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState(current_round_height))? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => round,
                // Case 2 - Storage failed to fetch the round height.
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        trace!("Preparing to lock chunk {}", chunk_id);

        // Attempt to acquire the chunk lock for participant.
        let contribution_locator = round.try_lock_chunk(&self.environment, &storage, chunk_id, &participant)?;

        trace!("Participant {} locked chunk {}", participant, chunk_id);

        // Add the updated round to storage.
        match storage.update(&Locator::RoundState(current_round_height), Object::RoundState(round)) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                info!("{} acquired lock on chunk {}", participant, chunk_id);
                storage.to_path(&contribution_locator)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
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
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        trace!("Current round height from storage is {}", current_round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match current_round_height != 0 {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState(current_round_height))? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => round,
                // Case 2 - Storage failed to fetch the round height.
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
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
            Locator::ContributionFile(current_round_height, chunk_id, next_contribution_id, false);
        let next_contribution = storage.to_path(&next_contribution_locator)?;

        // Add the next contribution to the current chunk.
        round.get_chunk_mut(chunk_id)?.add_contribution(
            next_contribution_id,
            participant,
            next_contribution.clone(),
            expected_num_contributions,
        )?;

        trace!("Next contribution locator is {}", next_contribution);
        {
            // TODO (howardwu): Check that the file size is nonzero, the structure is correct,
            //  and the starting hash is based on the previous contribution.
        }
        // Add the updated round to storage.
        match storage.update(&Locator::RoundState(current_round_height), Object::RoundState(round)) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                {
                    // TODO (howardwu): Send job to run verification on new chunk.
                }
                info!("{} added a contribution to chunk {}", participant, chunk_id);
                Ok(next_contribution)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
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
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        trace!("Current round height from storage is {}", current_round_height);

        // Check that the ceremony has started and fetch the current round from storage.
        let mut round = match current_round_height != 0 {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState(current_round_height))? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => round,
                // Case 2 - Storage failed to fetch the round height.
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
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
        if !storage.exists(&Locator::ContributionFile(
            current_round_height,
            chunk_id,
            current_contribution_id,
            false,
        )) {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Check if the current contribution has already been verified.
        if current_contribution.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch whether this is the final contribution of the specified chunk.
        let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());

        debug!("Coordinator is starting verification on chunk {}", chunk_id);
        Verification::run(
            &self.environment,
            &mut storage,
            current_round_height,
            chunk_id,
            current_contribution_id,
            is_final_contribution,
        )?;
        debug!("Coordinator completed verification on chunk {}", chunk_id);

        // Attempts to set the current contribution as verified in the current round.
        // Fetch the contribution locators for `Verification`.
        let verified_contribution_locator = match is_final_contribution {
            true => Locator::ContributionFile(current_round_height + 1, chunk_id, 0, true),
            false => Locator::ContributionFile(current_round_height, chunk_id, current_contribution_id, true),
        };
        round.verify_contribution(
            chunk_id,
            current_contribution_id,
            participant.clone(),
            storage.to_path(&verified_contribution_locator)?,
        )?;

        // Add the updated round to storage.
        match storage.update(&Locator::RoundState(current_round_height), Object::RoundState(round)) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                info!(
                    "{} verified chunk {} contribution {}",
                    participant, chunk_id, current_contribution_id
                );
                Ok(())
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
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
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        trace!("Current round height from storage is {}", current_round_height);

        // If this is the initial round, ensure the round does not exist yet.
        // Attempt to load the round corresponding to the given round height from storage.
        // If there is no round in storage, proceed to create a new round instance,
        // and run `Initialization` to start the ceremony.
        if current_round_height == 0 {
            // Check that the current round does not exist in storage.
            if storage.exists(&Locator::RoundState(current_round_height)) {
                return Err(CoordinatorError::RoundShouldNotExist);
            }

            // Check that the next round does not exist in storage.
            if storage.exists(&Locator::RoundState(current_round_height + 1)) {
                return Err(CoordinatorError::RoundShouldNotExist);
            }

            // Create an instantiation of `Round` for round 0.
            let round = {
                // Initialize the contributors as an empty list as this is for initialization.
                let contributors = vec![];

                // Initialize the verifiers as a list comprising only the coordinator verifier,
                // as this is for initialization.
                let verifiers = vec![self.environment.coordinator_verifier()];

                // Create a new round instance.
                Round::new(
                    &self.environment,
                    &storage,
                    current_round_height,
                    started_at,
                    contributors,
                    verifiers,
                )?
            };

            debug!("Starting initialization of round {}", current_round_height);

            // Execute initialization of contribution 0 for all chunks
            // in the new round and check that the new locators exist.
            for chunk_id in 0..self.environment.number_of_chunks() {
                // 1 - Check that the contribution locator corresponding to this round's chunk does not exist.
                let locator = Locator::ContributionFile(current_round_height, chunk_id, 0, true);
                if storage.exists(&locator) {
                    error!("Contribution locator already exists ({})", storage.to_path(&locator)?);
                    return Err(CoordinatorError::ContributionLocatorAlreadyExists);
                }

                // 2 - Check that the contribution locator corresponding to the next round's chunk does not exists.
                let locator = Locator::ContributionFile(current_round_height + 1, chunk_id, 0, true);
                if storage.exists(&locator) {
                    error!("Contribution locator already exists ({})", storage.to_path(&locator)?);
                    return Err(CoordinatorError::ContributionLocatorAlreadyExists);
                }

                info!("Coordinator is starting initialization on chunk {}", chunk_id);
                // TODO (howardwu): Add contribution hash to `Round`.
                let _contribution_hash =
                    Initialization::run(&self.environment, &mut storage, current_round_height, chunk_id)?;
                info!("Coordinator completed initialization on chunk {}", chunk_id);

                // 1 - Check that the contribution locator corresponding to this round's chunk now exists.
                let locator = Locator::ContributionFile(current_round_height, chunk_id, 0, true);
                if !storage.exists(&locator) {
                    error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                    return Err(CoordinatorError::ContributionLocatorMissing);
                }

                // 2 - Check that the contribution locator corresponding to the next round's chunk now exists.
                let locator = Locator::ContributionFile(current_round_height + 1, chunk_id, 0, true);
                if !storage.exists(&locator) {
                    error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                    return Err(CoordinatorError::ContributionLocatorMissing);
                }
            }

            // Add the new round to storage.
            storage.insert(Locator::RoundState(current_round_height), Object::RoundState(round))?;

            debug!("Updated round {} in storage", current_round_height);
            debug!("Completed initialization of round {}", current_round_height);
        }

        // Execute aggregation of the current round in preparation for
        // transitioning to the next round. If this is the initial round,
        // there should be nothing to aggregate and we may continue.
        if current_round_height != 0 {
            // Check that the current round exists in storage.
            if !storage.exists(&Locator::RoundState(current_round_height)) {
                return Err(CoordinatorError::RoundLocatorMissing);
            }

            // Check that the next round does not exist in storage.
            if storage.exists(&Locator::RoundState(current_round_height + 1)) {
                return Err(CoordinatorError::RoundShouldNotExist);
            }

            // TODO (howardwu): Check that all locks have been released.

            // Check that the ceremony has started and fetch the current round from storage.
            let mut round = match current_round_height != 0 {
                // Load the corresponding round data from storage.
                true => match storage.get(&Locator::RoundState(current_round_height))? {
                    // Case 1 - The ceremony is running and the round state was fetched.
                    Object::RoundState(round) => round,
                    // Case 2 - Storage failed to fetch the round height.
                    _ => return Err(CoordinatorError::StorageFailed),
                },
                // Case 3 - There are no prior rounds of the ceremony.
                false => return Err(CoordinatorError::RoundDoesNotExist),
            };

            // Check that all chunks in the current round are verified,
            // so that we may transition to the next round.
            if !&round.is_complete() {
                error!(
                    "Round {} is not complete and next round is not starting",
                    current_round_height
                );
                trace!("{:#?}", &round);
                return Err(CoordinatorError::RoundNotComplete);
            }

            // Execute round aggregation and aggregate verification on the current round.
            {
                let contribution_id = round.expected_number_of_contributions() - 1;
                for chunk_id in 0..self.environment.number_of_chunks() {
                    // Check that the final unverified contribution locator exists.
                    let locator = Locator::ContributionFile(current_round_height, chunk_id, contribution_id, false);
                    if !storage.exists(&locator) {
                        error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                        return Err(CoordinatorError::ContributionMissing);
                    }
                    // Check that the final verified contribution locator exists.
                    let locator = Locator::ContributionFile(current_round_height + 1, chunk_id, 0, true);
                    if !storage.exists(&locator) {
                        error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                        return Err(CoordinatorError::ContributionMissing);
                    }
                }

                // Check that the round locator does not exist.
                let round_locator = Locator::RoundFile(current_round_height);
                if storage.exists(&round_locator) {
                    error!("Round locator already exists ({})", storage.to_path(&round_locator)?);
                    return Err(CoordinatorError::RoundLocatorAlreadyExists);
                }

                // TODO (howardwu): Add aggregate verification logic.
                // Execute aggregation to combine on all chunks to finalize the round
                // corresponding to the given round height.
                debug!("Coordinator is starting aggregation");
                Aggregation::run(&self.environment, &storage, &round)?;
                debug!("Coordinator completed aggregation");

                // Check that the round locator now exists.
                if !storage.exists(&round_locator) {
                    error!("Round locator is missing ({})", storage.to_path(&round_locator)?);
                    return Err(CoordinatorError::RoundLocatorMissing);
                }
            }
        }

        // Create the new round height.
        let new_height = current_round_height + 1;

        info!(
            "Starting transition from round {} to {}",
            current_round_height, new_height
        );

        // Check that the new round does not exist in storage.
        // If it exists, this means the round was already initialized.
        let locator = Locator::RoundState(new_height);
        if storage.exists(&locator) {
            error!("Round {} already exists ({})", new_height, storage.to_path(&locator)?);
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Check that each contribution for the next round exists.
        for chunk_id in 0..self.environment.number_of_chunks() {
            debug!("Locating round {} chunk {} contribution 0", new_height, chunk_id);
            let locator = Locator::ContributionFile(new_height, chunk_id, 0, true);
            if !storage.exists(&locator) {
                error!("Contribution locator is missing ({})", storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Instantiate the new round and height.
        let new_round = Round::new(
            &self.environment,
            &storage,
            new_height,
            started_at,
            contributors,
            verifiers,
        )?;

        #[cfg(test)]
        trace!("{:#?}", &new_round);

        // Insert the new round into storage.
        storage.insert(Locator::RoundState(new_height), Object::RoundState(new_round))?;

        // Next, update the round height to reflect the new round.
        storage.update(&Locator::RoundHeight, Object::RoundHeight(new_height))?;

        debug!("Added round {} to storage", current_round_height);
        info!(
            "Completed transition from round {} to {}",
            current_round_height, new_height
        );
        Ok(new_height)
    }

    ///
    /// Returns a reference to the instantiation of `Environment` that this
    /// coordinator is using.
    ///
    #[inline]
    pub(crate) fn environment(&self) -> &Environment {
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

        // TODO (howardwu): Switch to a storage reader.
        // Acquire the storage lock.
        let storage = self.storage.write().unwrap();

        // Fetch the current round height from storage.
        let current_round_height = match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(round_height) => round_height,
            // Case 2 - Storage failed to fetch the round height.
            _ => return Err(CoordinatorError::StorageFailed),
        };

        // Check that the given round height is valid.
        let round = match round_height != 0 && round_height <= current_round_height {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState(current_round_height))? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => round,
                // Case 2 - Storage failed to fetch the round height.
                _ => return Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => return Err(CoordinatorError::RoundDoesNotExist),
        };

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, &participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the contribution locator corresponding to this round and chunk exists.
        let locator = Locator::ContributionFile(round_height, chunk_id, contribution_id, false);
        if storage.exists(&locator) {
            error!("Contribution locator already exists ({})", storage.to_path(&locator)?);
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
        Computation::run(&self.environment, &storage, round_height, chunk_id, contribution_id)?;
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
        test_logger();
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
        test_logger();
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
