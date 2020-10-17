use crate::{
    commands::{Aggregation, Computation, Initialization, Verification},
    environment::Environment,
    objects::{participant::*, Round},
    storage::{Locator, Object, Storage, StorageLock},
    CoordinatorState,
};
use setup_utils::calculate_hash;

use crate::commands::{Seed, SEED_LENGTH};
use chrono::{DateTime, Utc};
use rand::RngCore;
use secrecy::{ExposeSecret, SecretVec};
use std::{
    convert::TryInto,
    fmt,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{task, time::delay_for};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub enum CoordinatorError {
    AggregateContributionFileSizeMismatch,
    ChunkAlreadyComplete,
    ChunkAlreadyVerified,
    ChunkIdAlreadyAdded,
    ChunkIdInvalid,
    ChunkIdMismatch,
    ChunkIdMissing,
    ChunkLockAlreadyAcquired,
    ChunkLockLimitReached,
    ChunkMissing,
    ChunkMissingVerification,
    ChunkNotLockedOrByWrongParticipant,
    ComputationFailed,
    CompressedContributionHashingUnsupported,
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFileSizeMismatch,
    ContributionHashMismatch,
    ContributionIdIsNonzero,
    ContributionIdMismatch,
    ContributionIdMustBeNonzero,
    ContributionLocatorAlreadyExists,
    ContributionLocatorIncorrect,
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
    LocatorDeserializationFailed,
    LocatorFileAlreadyExists,
    LocatorFileAlreadyExistsAndOpen,
    LocatorFileAlreadyOpen,
    LocatorFileMissing,
    LocatorFileNotOpen,
    LocatorFileShouldBeOpen,
    LocatorSerializationFailed,
    NextRoundAlreadyInPrecommit,
    NextRoundShouldBeEmpty,
    NumberOfChunksInvalid,
    NumberOfContributionsDiffer,
    ParticipantAlreadyAdded,
    ParticipantAlreadyAddedChunk,
    ParticipantAlreadyDropped,
    ParticipantAlreadyFinished,
    ParticipantAlreadyFinishedChunk,
    ParticipantAlreadyWorkingOnChunk,
    ParticipantAlreadyStarted,
    ParticipantBanned,
    ParticipantDidNotWork,
    ParticipantDidntLockChunkId,
    ParticipantHasNotStarted,
    ParticipantHasNoRemainingChunks,
    ParticipantHasRemainingChunks,
    ParticipantMissing,
    ParticipantNotFound,
    ParticipantNotReady,
    ParticipantRoundHeightInvalid,
    ParticipantRoundHeightMissing,
    ParticipantStillHasLocks,
    ParticipantUnauthorized,
    ParticipantUnauthorizedForChunkId,
    ParticipantWasDropped,
    Phase1Setup(setup_utils::Error),
    QueueIsEmpty,
    RoundAggregationFailed,
    RoundAlreadyInitialized,
    RoundContributorsMissing,
    RoundContributorsNotUnique,
    RoundDirectoryMissing,
    RoundDoesNotExist,
    RoundFileSizeMismatch,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundHeightNotSet,
    RoundLocatorAlreadyExists,
    RoundLocatorMissing,
    RoundNotComplete,
    RoundNotReady,
    RoundNumberOfContributorsUnauthorized,
    RoundNumberOfVerifiersUnauthorized,
    RoundShouldNotExist,
    RoundVerifiersMissing,
    RoundVerifiersNotUnique,
    StorageCopyFailed,
    StorageFailed,
    StorageInitializationFailed,
    StorageLocatorAlreadyExists,
    StorageLocatorAlreadyExistsAndOpen,
    StorageLocatorFormatIncorrect,
    StorageLocatorMissing,
    StorageLocatorNotOpen,
    StorageLockFailed,
    StorageReaderFailed,
    StorageSizeLookupFailed,
    StorageUpdateFailed,
    TryFromSliceError(std::array::TryFromSliceError),
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    Url(url::ParseError),
    VerificationFailed,
    VerificationOnContributionIdZero,
    VerifierMissing,
    VerifiersMissing,
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

impl From<setup_utils::Error> for CoordinatorError {
    fn from(error: setup_utils::Error) -> Self {
        CoordinatorError::Phase1Setup(error)
    }
}

impl From<std::array::TryFromSliceError> for CoordinatorError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        CoordinatorError::TryFromSliceError(error)
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
#[derive(Clone)]
pub struct Coordinator {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The storage of contributions and rounds for this coordinator.
    storage: Arc<RwLock<Box<dyn Storage>>>,
    /// The current round and participant state.
    state: Arc<RwLock<CoordinatorState>>,
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
            environment: environment.clone(),
            storage: Arc::new(RwLock::new(environment.storage()?)),
            state: Arc::new(RwLock::new(CoordinatorState::new(environment))),
        })
    }

    ///
    /// Runs a set of operations to initialize state and start the coordinator.
    ///
    #[inline]
    pub async fn initialize(&self) -> Result<(), CoordinatorError> {
        info!("Coordinator is booting up");

        // TODO (howardwu): Isolate this and persist it securely on device.
        let seed = {
            // Load or initialize the seed for the contributor of the coordinator.
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            SecretVec::new(seed.to_vec())
        };

        // Fetch the current round height from storage.
        let current_round_height = self.current_round_height()?;

        // If this is a new ceremony, execute the first round to initialize the ceremony.
        if current_round_height == 0 {
            // Fetch the contributor and verifier of the coordinator.
            let contributor = self
                .environment
                .coordinator_contributors()
                .first()
                .ok_or(CoordinatorError::ContributorsMissing)?;
            let verifier = self
                .environment
                .coordinator_verifiers()
                .first()
                .ok_or(CoordinatorError::VerifiersMissing)?;

            {
                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

                // Set the current round height to initialize coordinator state.
                state.set_current_round_height(current_round_height);

                // Add the contributor and verifier of the coordinator to execute round 1.
                state.add_to_queue(contributor.clone(), 10)?;
                state.add_to_queue(verifier.clone(), 10)?;

                // Update the state of the queue.
                state.update_queue()?;

                // Drop the state write lock.
                drop(state);
            }

            info!("Initializing round 1");
            self.try_advance()?;
            info!("Initialized round 1");

            info!("Add contributions and verifications for round 1");
            for _ in 0..self.environment.number_of_chunks() {
                {
                    let (chunk_id, _previous_response, _challenge, response) = self.try_lock(&contributor)?;

                    debug!("Computing contributions for round 1 chunk {}", chunk_id);
                    self.run_computation(1, chunk_id, 1, &contributor, seed.expose_secret()[..].try_into()?)?;
                    let _response = self.try_contribute(&contributor, &response)?;
                    debug!("Computed contributions for round 1 chunk {}", chunk_id);
                }

                {
                    let (chunk_id, _challenge, _response, next_challenge) = self.try_lock(&verifier)?;

                    debug!("Running verification for round 1 chunk {}", chunk_id);
                    let _next_challenge = self.run_verification(1, chunk_id, 1, &verifier)?;
                    self.try_verify(&verifier, &next_challenge)?;
                    debug!("Running verification for round 1 chunk {}", chunk_id);
                }
            }
            info!("Added contributions and verifications for round 1");
        } else {
            // Acquire the state write lock.
            let mut state = self.state.write().unwrap();

            // Set the current round height to initialize coordinator state.
            state.set_current_round_height(current_round_height);

            // Drop the state write lock.
            drop(state);
        }

        info!("{}", serde_json::to_string_pretty(&self.current_round()?)?);

        // Clone the coordinator.
        let coordinator = self.clone();

        // Start a task thread for the coordinator.
        task::spawn(async move {
            // Initialize the coordinator loop.
            loop {
                // Run the update operation.
                coordinator.update().await.unwrap();

                // Sleep for 10 seconds in between iterations.
                delay_for(Duration::from_secs(10)).await;
            }
        });

        info!("Coordinator has booted up");
        Ok(())
    }

    #[inline]
    pub fn shutdown_listener(self) -> anyhow::Result<()> {
        ctrlc::set_handler(move || {
            warn!("\n\nATTENTION - Coordinator is shutting down...\n");

            // Acquire the coordinator state lock.
            let state = self.state.write().unwrap();
            debug!("Coordinator has safely locked state");

            // Print the final coordinator state.
            let final_state = serde_json::to_string_pretty(&*state).unwrap();
            info!("\n\nCoordinator State at Shutdown\n\n{}\n", final_state);

            // Acquire the storage lock.
            let _storage = self.storage.write().unwrap();
            debug!("Coordinator has safely shutdown storage");

            info!("\n\nCoordinator has safely shutdown.\n\nGoodbye.\n");
            std::process::exit(0);
        })?;

        Ok(())
    }

    ///
    /// Runs a set of operations to update the coordinator state to reflect
    /// newly finished, dropped, or banned participants.
    ///
    #[inline]
    pub async fn update(&self) -> Result<(), CoordinatorError> {
        // Process updates for the current round and check if the current round is finished.
        let (is_current_round_finished, is_next_round_ready) = {
            // Acquire the state write lock.
            let mut state = self.state.write().unwrap();

            info!("\n\t{}", state.status_report());

            // Update the state of the queue.
            state.update_queue()?;

            // Update the state of current round contributors.
            state.update_current_contributors()?;

            // Update the state of current round verifiers.
            state.update_current_verifiers()?;

            // Drop disconnected participants from the current round.
            state.update_dropped_participants()?;

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants()?;

            // Determine if current round is finished and precommit to next round is ready.
            (state.is_current_round_finished(), state.is_precommit_next_round_ready())
        };

        trace!("{} {}", is_current_round_finished, is_next_round_ready);

        // Check if the current round is finished and the next round is ready.
        if is_current_round_finished && is_next_round_ready {
            // Check that all locators exist.

            // Backup a copy of the current coordinator.

            // Attempt to advance to the next round.
            let next_round_height = self.try_advance()?;

            trace!("Advanced ceremony to round {}", next_round_height);
        }

        Ok(())
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// contributor and listed in the contributor IDs for this round.
    ///
    /// If the participant is not a contributor, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant: &Participant) -> bool {
        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        let round = match Self::load_current_round(&storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return false,
        };

        // Release the storage read lock.
        drop(storage);

        // Check that the participant is a contributor for the given round height.
        if !round.is_contributor(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();

        // Check that the participant is an authorized contributor.
        if !state.is_authorized_contributor(participant) {
            return false;
        }

        true
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// verifier and listed in the verifier IDs for this round.
    ///
    /// If the participant is not a verifier, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_current_verifier(&self, participant: &Participant) -> bool {
        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        let round = match Self::load_current_round(&storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return false,
        };

        // Release the storage read lock.
        drop(storage);

        // Check that the participant is a verifier for the given round height.
        if !round.is_verifier(participant) {
            return false;
        }

        // Acquire a state read lock.
        let state = self.state.read().unwrap();

        // Check that the participant is an authorized contributor.
        if !state.is_authorized_verifier(participant) {
            return false;
        }

        true
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///  
    #[inline]
    pub fn number_of_queued_contributors(&self) -> usize {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the number of queued contributors.
        state.number_of_queued_contributors()
    }

    ///
    /// Returns the total number of verifiers currently in the queue.
    ///
    #[inline]
    pub fn number_of_queued_verifiers(&self) -> usize {
        // Acquire a state read lock.
        let state = self.state.read().unwrap();
        // Fetch the number of queued verifiers.
        state.number_of_queued_verifiers()
    }

    ///
    /// Adds the given participant to the queue if they are permitted to participate.
    ///
    #[inline]
    pub fn add_to_queue(&self, participant: Participant, reliability_score: u8) -> Result<(), CoordinatorError> {
        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();
        // Attempt to add the participant to the next round.
        state.add_to_queue(participant, reliability_score)
    }

    ///
    /// Removes the given participant from the queue if they are in the queue.
    ///
    #[inline]
    pub fn remove_from_queue(&self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();
        // Attempt to remove the participant from the next round.
        state.remove_from_queue(participant)
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
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

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
    /// Returns the current round state of the ceremony from storage,
    /// irrespective of the stage of its completion.
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

        // Acquire a storage read lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round from storage.
        Self::load_current_round(&storage)
    }

    ///
    /// Returns the round state corresponding to the given height from storage.
    ///
    /// If there are no prior rounds, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn get_round(&self, round_height: u64) -> Result<Round, CoordinatorError> {
        // Acquire the storage lock.
        let storage = StorageLock::Read(self.storage.read().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        // Check that the given round height is valid.
        match round_height <= current_round_height {
            // Fetch the round corresponding to the given round height from storage.
            true => Ok(serde_json::from_slice(
                &*storage.reader(&Locator::RoundState(round_height))?.as_ref(),
            )?),
            // The given round height does not exist.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Attempts to acquire the lock to a chunk for the given participant.
    ///
    /// On success, if the participant is a contributor, this function
    /// returns `(chunk_id, previous_response_locator, challenge_locator, response_locator)`.
    ///
    /// On success, if the participant is a verifier, this function
    /// returns `(chunk_id, challenge_locator, response_locator, next_challenge_locator)`.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_lock(&self, participant: &Participant) -> Result<(u64, String, String, String), CoordinatorError> {
        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Attempt to fetch the next chunk ID for the given participant.
        trace!("Fetching next chunk ID for {}", participant);
        let chunk_id = state.pop_chunk_id(participant)?;

        trace!("Trying to lock chunk {} for {}", chunk_id, participant);
        match self.try_lock_chunk(chunk_id, participant) {
            // Case 1 - Participant acquired lock, return the locator.
            Ok((previous_contribution_locator, current_contribution_locator, next_contribution_locator)) => {
                trace!("Incrementing the number of locks held by {}", participant);
                state.acquired_lock(participant, chunk_id)?;

                info!("Acquired lock on chunk {} for {}", chunk_id, participant);
                Ok((
                    chunk_id,
                    previous_contribution_locator,
                    current_contribution_locator,
                    next_contribution_locator,
                ))
            }
            // Case 2 - Participant failed to acquire the lock, put the chunk ID back.
            Err(error) => {
                trace!("Failed to acquire lock and adding chunk ID back to participant queue");
                state.push_front_chunk_id(participant, chunk_id)?;

                error!("{}", error);
                return Err(error);
            }
        }
    }

    ///
    /// Attempts to add a contribution for the given unverified response file locator
    /// from the given participant.
    ///
    /// On success, this function releases the lock from the contributor and returns
    /// the response file locator.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_contribute(
        &self,
        participant: &Participant,
        unverified_locator: &str,
    ) -> Result<String, CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Fetch the chunk ID from the response file locator.
        let chunk_id = match self.storage.read().unwrap().to_locator(unverified_locator)? {
            Locator::ContributionFile(_round_height, chunk_id, _contribution_id, verified) => {
                // Check that the response file locator is unverified.
                if verified == true {
                    error!("{} provided a verified locator {}", participant, unverified_locator);
                    return Err(CoordinatorError::ContributionLocatorIncorrect);
                }

                // TODO (howardwu): Check that the given locator corresponds to the correct response file.
                chunk_id
            }
            _ => {
                error!("{} provided an irrelevant locator {}", participant, unverified_locator);
                return Err(CoordinatorError::ContributionLocatorIncorrect);
            }
        };

        trace!("Trying to add contribution from {} for chunk {}", chunk_id, participant);
        match self.add_contribution(chunk_id, participant) {
            // Case 1 - Participant added contribution, return the response file locator.
            Ok(locator) => {
                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

                trace!("Release the lock on chunk {} from {}", chunk_id, participant);
                state.released_lock(participant, chunk_id)?;

                info!("Added contribution for chunk {} at {}", chunk_id, locator);
                Ok(locator)
            }
            // Case 2 - Participant failed to add their contribution, remove the contribution file.
            Err(error) => {
                trace!("Failed to add a contribution and removing the contribution file");
                // self.state.push_chunk_id(participant, chunk_id)?;

                // TODO (howardwu): Delete the response file.

                error!("{}", error);
                return Err(error);
            }
        }
    }

    ///
    /// Attempts to add a verification with the given verified response file locator
    /// from the given participant.
    ///
    #[inline]
    pub fn try_verify(&self, participant: &Participant, verified_locator: &str) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Fetch the chunk ID from the response file locator.
        let chunk_id = match self.storage.read().unwrap().to_locator(verified_locator)? {
            Locator::ContributionFile(_round_height, chunk_id, _contribution_id, verified) => {
                // Check that the response file locator is verified.
                if verified == false {
                    error!("{} provided an unverified locator {}", participant, verified_locator);
                    return Err(CoordinatorError::ContributionLocatorIncorrect);
                }

                // TODO (howardwu): Load a reader for the unverified response file and verified file.
                // TODO (howardwu): Check that the given locator corresponds to the correct response file.

                chunk_id
            }
            _ => {
                error!("{} provided an irrelevant locator {}", participant, verified_locator);
                return Err(CoordinatorError::ContributionLocatorIncorrect);
            }
        };

        trace!("Trying to add verification from {} for chunk {}", chunk_id, participant);
        match self.verify_contribution(chunk_id, participant) {
            // Case 1 - Participant verified contribution, return the response file locator.
            Ok(locator) => {
                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

                trace!("Decrementing the number of locks held by {}", participant);
                state.released_lock(participant, chunk_id)?;

                info!("Added verification for chunk {}", chunk_id);
                Ok(locator)
            }
            // Case 2 - Participant failed to add their contribution, remove the contribution file.
            Err(error) => {
                trace!("Failed to add a verification and removing the contribution file");
                // self.state.push_chunk_id(participant, chunk_id)?;

                // TODO (howardwu): Delete the response file.

                error!("{}", error);
                return Err(error);
            }
        }
    }

    ///
    /// Attempts to advance the ceremony to the next round.
    ///
    #[inline]
    pub fn try_advance(&self) -> Result<u64, CoordinatorError> {
        // Acquire the state write lock.
        let mut state = self.state.write().unwrap();

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        match state.precommit_next_round(self.current_round_height()? + 1) {
            // Case 1 - Precommit succeed, attempt to advance the round.
            Ok((contributors, verifiers)) => {
                // Fetch the current time.
                let now = Utc::now();

                trace!("Trying to add advance to the next round");
                match self.next_round(now, contributors, verifiers) {
                    // Case 1a - Coordinator advanced the round.
                    Ok(next_round_height) => {
                        // If success, update coordinator state to next round.
                        info!("Coordinator has advanced to round {}", next_round_height);
                        state.commit_next_round();
                        Ok(next_round_height)
                    }
                    // Case 1b - Coordinator failed to advance the round.
                    Err(error) => {
                        // If failed, rollback coordinator state to the current round.
                        error!("Coordinator failed to advance the next round, performing state rollback");
                        state.rollback_next_round();
                        error!("{}", error);
                        Err(error)
                    }
                }
            }
            // Case 2 - Precommit failed, roll back precommit.
            Err(error) => {
                // If failed, rollback coordinator state to the current round.
                error!("Precommit for next round has failed, performing state rollback");
                state.rollback_next_round();
                error!("{}", error);
                Err(error)
            }
        }
    }
}

impl Coordinator {
    ///
    /// Attempts to acquire the lock for a given chunk ID and participant.
    ///
    /// On success, if the participant is a contributor, this function
    /// returns `(chunk_id, previous_response_locator, challenge_locator, response_locator)`.
    ///
    /// On success, if the participant is a verifier, this function
    /// returns `(chunk_id, challenge_locator, response_locator, next_challenge_locator)`.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn try_lock_chunk(
        &self,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<(String, String, String), CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Acquire the storage lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;

        // Attempt to acquire the chunk lock for participant.
        trace!("Preparing to lock chunk {}", chunk_id);
        let (previous_contribution_locator, current_contribution_locator, next_contribution_locator) =
            round.try_lock_chunk(&self.environment, &mut storage, chunk_id, &participant)?;
        trace!("Participant {} locked chunk {}", participant, chunk_id);

        // Add the updated round to storage.
        match storage.update(&Locator::RoundState(current_round_height), Object::RoundState(round)) {
            Ok(_) => {
                info!("{} acquired lock on chunk {}", participant, chunk_id);
                Ok((
                    storage.to_path(&previous_contribution_locator)?,
                    storage.to_path(&current_contribution_locator)?,
                    storage.to_path(&next_contribution_locator)?,
                ))
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Attempts to add a contribution for a given chunk ID from a given participant.
    ///
    /// This function assumes the participant is a contributor and has just uploaded
    /// their response file to the coordinator. The coordinator proceeds to sanity check
    /// (however, does not verify) the contribution before accepting the response file.
    ///
    /// On success, this function releases the chunk lock from the contributor and
    /// returns the response file locator.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn add_contribution(
        &self,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<String, CoordinatorError> {
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
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;

        // Check that the participant is an authorized contributor to the current round.
        if !round.is_contributor(participant) {
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
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the next contribution ID of the chunk.
        let contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the next contribution ID is one above the current contribution ID.
        if !chunk.is_next_contribution_id(contribution_id, expected_num_contributions) {
            return Err(CoordinatorError::ContributionIdMismatch);
        }

        // Fetch the challenge and response locators.
        let challenge_file_locator =
            Locator::ContributionFile(current_round_height, chunk_id, chunk.current_contribution_id(), true);
        let response_file_locator = Locator::ContributionFile(current_round_height, chunk_id, contribution_id, false);

        {
            // Fetch a challenge file reader.
            let challenge_reader = storage.reader(&challenge_file_locator)?;
            trace!("Challenge is located in {}", storage.to_path(&challenge_file_locator)?);

            // Fetch a response file reader.
            let response_reader = storage.reader(&response_file_locator)?;
            trace!("Response is located in {}", storage.to_path(&response_file_locator)?);

            // Compute the challenge hash using the challenge file.
            let challenge_hash = calculate_hash(challenge_reader.as_ref());
            debug!("Challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

            // Fetch the challenge hash from the response file.
            let challenge_hash_in_response = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];
            let pretty_hash = pretty_hash!(&challenge_hash_in_response);
            debug!("Challenge hash in response file is {}", pretty_hash);

            // Check the starting hash in the response file is based on the previous contribution.
            if challenge_hash_in_response != challenge_hash.as_slice() {
                error!("Challenge hash in response file does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        // Add the contribution response to the current chunk.
        round
            .chunk_mut(chunk_id)?
            .add_contribution(participant, storage.to_path(&response_file_locator)?)?;

        // Add the updated round to storage.
        match storage.update(&Locator::RoundState(current_round_height), Object::RoundState(round)) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                {
                    // TODO (howardwu): Send job to run verification on new chunk.
                }
                info!("{} added a contribution to chunk {}", participant, chunk_id);
                Ok(storage.to_path(&response_file_locator)?)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Attempts to run verification in the current round for a given chunk ID and participant.
    ///
    /// On success, this function stores the next challenge locator into the round transcript
    /// and releases the chunk lock from the verifier.
    ///
    #[inline]
    pub(crate) fn verify_contribution(&self, chunk_id: u64, participant: &Participant) -> Result<(), CoordinatorError> {
        info!("Attempting to verify a contribution for chunk {}", chunk_id);
        {
            // Check that the chunk ID is valid.
            if chunk_id > self.environment.number_of_chunks() {
                return Err(CoordinatorError::ChunkIdInvalid);
            }

            // Check that the participant is a verifier.
            if !participant.is_verifier() {
                return Err(CoordinatorError::ExpectedVerifier);
            }
        }

        // Acquire the storage lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;
        {
            // Check that the participant is an authorized verifier to the current round.
            if !round.is_verifier(participant) {
                error!("{} is unauthorized to verify chunk {})", participant, chunk_id);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the chunk lock is currently held by this verifier.
            if !round.is_chunk_locked_by(chunk_id, participant) {
                error!("{} should have lock on chunk {} but does not", participant, chunk_id);
                return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
            }
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();

        // Fetch the next challenge locator.
        let next_challenge = {
            // Fetch whether this is the final contribution of the specified chunk.
            let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());
            match is_final_contribution {
                true => Locator::ContributionFile(current_round_height + 1, chunk_id, 0, true),
                false => Locator::ContributionFile(current_round_height, chunk_id, current_contribution_id, true),
            }
        };

        {
            // Compute the response hash.
            let response_hash = calculate_hash(&storage.reader(&Locator::ContributionFile(
                current_round_height,
                chunk_id,
                current_contribution_id,
                false,
            ))?);

            // Fetch the saved response hash in the next challenge file.
            let saved_response_hash = storage
                .reader(&next_challenge)?
                .as_ref()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();

            // Check that the response hash matches the next challenge hash.
            debug!("The response hash is {}", pretty_hash!(&response_hash));
            debug!("The saved response hash is {}", pretty_hash!(&saved_response_hash));
            if response_hash.as_slice() != saved_response_hash {
                error!("Response hash does not match the saved response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        // Sets the current contribution as verified in the current round.
        round.verify_contribution(
            chunk_id,
            current_contribution_id,
            participant.clone(),
            storage.to_path(&next_challenge)?,
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
    pub(crate) fn next_round(
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
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

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

                // Initialize the verifiers as a list comprising only one coordinator verifier,
                // as this is for initialization.
                let verifiers = vec![
                    self.environment
                        .coordinator_verifiers()
                        .first()
                        .ok_or(CoordinatorError::VerifierMissing)?
                        .clone(),
                ];

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

            // Fetch the current round from storage.
            let round = Self::load_current_round(&storage)?;

            // Check that all chunks in the current round are verified,
            // so that we may transition to the next round.
            if !round.is_complete() {
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
                Aggregation::run(&self.environment, &mut storage, &round)?;
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
    #[allow(dead_code)]
    #[inline]
    pub(super) fn environment(&self) -> &Environment {
        &self.environment
    }

    ///
    /// Returns a reference to the instantiation of `Storage` that this
    /// coordinator is using.
    ///
    #[allow(dead_code)]
    #[inline]
    pub(super) fn storage(&self) -> Arc<RwLock<Box<dyn Storage>>> {
        self.storage.clone()
    }

    ///
    /// Attempts to run computation for a given round height, given chunk ID, and contribution ID.
    ///
    /// This function is used for testing purposes and can also be used for completing contributions
    /// of participants who may have dropped off and handed over control of their session.
    ///
    #[allow(dead_code)]
    #[inline]
    pub(super) fn run_computation(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
        seed: &Seed,
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

        // TODO (howardwu): Switch to a storage read lock.
        // Acquire the storage lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

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

        // Fetch the current round and given chunk ID, and check that
        // the given contribution ID does not exist yet.
        let chunk = round.chunk(chunk_id)?;
        if chunk.get_contribution(contribution_id).is_ok() {
            return Err(CoordinatorError::ContributionShouldNotExist);
        }

        // Fetch the challenge locator.
        let challenge_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id - 1, true);
        // Fetch the response locator.
        let response_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id, false);

        info!(
            "Starting computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Computation::run(
            &self.environment,
            &mut storage,
            challenge_locator,
            response_locator,
            seed,
        )?;
        info!(
            "Completed computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        Ok(())
    }

    ///
    /// Attempts to run verification for a given round height, given chunk ID, and contribution ID.
    ///
    /// On success, this function returns the verified contribution locator.
    ///
    /// This function copies the unverified contribution into the verified contribution locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    #[allow(dead_code)]
    #[inline]
    pub(super) fn run_verification(
        &self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
    ) -> Result<String, CoordinatorError> {
        info!(
            "Running verification for round {} chunk {} contribution {} as {}",
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

        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // TODO (howardwu): Switch to a storage read lock.
        // Acquire the storage lock.
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

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

        // Check that the contribution locator corresponding to the response file exists.
        let response_locator = Locator::ContributionFile(round_height, chunk_id, contribution_id, false);
        if !storage.exists(&response_locator) {
            error!("Response file at {} is missing", storage.to_path(&response_locator)?);
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;

        // Fetch the contribution corresponding to the given contribution ID.
        let contribution = chunk.get_contribution(contribution_id)?;

        // Check if the contribution has NOT been verified yet.
        if contribution.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch whether this is the final contribution of the specified chunk.
        let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());

        // Fetch the verified response locator.
        let verified_locator = match is_final_contribution {
            true => Locator::ContributionFile(round_height + 1, chunk_id, 0, true),
            false => Locator::ContributionFile(round_height, chunk_id, contribution_id, true),
        };

        info!(
            "Starting verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Verification::run(
            &self.environment,
            &mut storage,
            round_height,
            chunk_id,
            contribution_id,
            is_final_contribution,
        )?;
        info!(
            "Completed verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the verified contribution locator exists.
        if !storage.exists(&verified_locator) {
            let verified_response = storage.to_path(&verified_locator)?;
            error!("Verified response file at {} is missing", verified_response);
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        Ok(storage.to_path(&verified_locator)?)
    }

    #[inline]
    fn load_current_round_height(storage: &StorageLock) -> Result<u64, CoordinatorError> {
        // Fetch the current round height from storage.
        match storage.get(&Locator::RoundHeight)? {
            // Case 1 - This is a typical round of the ceremony.
            Object::RoundHeight(current_round_height) => Ok(current_round_height),
            // Case 2 - Storage failed to fetch the round height.
            _ => Err(CoordinatorError::StorageFailed),
        }
    }

    #[inline]
    fn load_current_round(storage: &StorageLock) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(storage)?;

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
}

#[cfg(test)]
mod tests {
    use crate::{environment::*, testing::prelude::*, Coordinator};

    use crate::commands::{Seed, SEED_LENGTH};
    use chrono::Utc;
    use once_cell::sync::Lazy;
    use rand::RngCore;
    use std::{collections::HashMap, panic, process};

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

    fn coordinator_initialization_matches_json_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT.clone())?;
        initialize_coordinator(&coordinator)?;

        // Check that round 0 matches the round 0 JSON specification.
        {
            // Fetch round 0 from coordinator.
            let expected = test_round_0_json()?;
            let candidate = coordinator.get_round(0)?;
            print_diff(&expected, &candidate);
            assert_eq!(expected, candidate);
        }

        Ok(())
    }

    fn coordinator_initialization_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

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
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID_2));
            assert!(!current_round.is_contributor(&TEST_CONTRIBUTOR_ID_3));
            assert!(!current_round.is_contributor(&TEST_VERIFIER_ID));

            // Check round 1 verifiers.
            assert_eq!(1, current_round.number_of_verifiers());
            assert!(current_round.is_verifier(&TEST_VERIFIER_ID));
            assert!(!current_round.is_verifier(&TEST_VERIFIER_ID_2));
            assert!(!current_round.is_verifier(&TEST_CONTRIBUTOR_ID));

            // Check round 1 is NOT complete.
            assert!(!current_round.is_complete());
        }

        Ok(())
    }

    fn coordinator_contributor_try_lock_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

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
            let chunk = round.chunk(0)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 1 is locked.
            let chunk = round.chunk(1)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 1 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 2 is locked.
            let chunk = round.chunk(2)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 2 is locked by contributor 2.
            assert!(chunk.is_locked_by(contributor_2));
            assert!(!chunk.is_locked_by(contributor));
        }

        Ok(())
    }

    fn coordinator_contributor_add_contribution_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

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
            let chunk = round.chunk(chunk_id)?;
            assert!(!chunk.is_complete(round.expected_number_of_contributions()));

            // Check next contribution is 1.
            let contribution_id = 1;
            assert!(chunk.is_next_contribution_id(contribution_id, round.expected_number_of_contributions()));

            // Run the computation
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            assert!(
                coordinator
                    .run_computation(round_height, chunk_id, contribution_id, &contributor, &seed)
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
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_unlocked());
            assert!(!chunk.is_locked());
        }

        Ok(())
    }

    fn coordinator_verifier_verify_contribution_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

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
        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
        assert!(
            coordinator
                .run_computation(round_height, chunk_id, contribution_id, contributor, &seed)
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
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by the verifier.
            debug!("{:#?}", round);
            assert!(chunk.is_locked_by(&verifier));
        }

        // Verify round 1 chunk 0 contribution 1.
        {
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();

            // Run verification.
            let verify = coordinator.run_verification(round_height, chunk_id, contribution_id, &verifier);
            assert!(verify.is_ok());

            // Verify contribution 1.
            coordinator.verify_contribution(chunk_id, &verifier)?;
        }

        Ok(())
    }

    // This test runs a round with a single coordinator and single verifier
    // The verifier instances are run on a separate thread to simulate an environment where
    // verification and contribution happen concurrently.
    fn coordinator_concurrent_contribution_verification_test() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

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
        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
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
                let contribute =
                    coordinator.run_computation(round_height, chunk_id, contribution_id, &contributor, &seed);
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

                // Run verification as verifier.
                let verify = coordinator_clone.run_verification(round_height, chunk_id, contribution_id, &verifier);
                if verify.is_err() {
                    error!(
                        "Failed to run verification as verifier {:?}\n{}",
                        &verifier,
                        serde_json::to_string_pretty(&coordinator_clone.current_round().unwrap()).unwrap()
                    );
                    verify.unwrap();
                }

                // Add the verification as the verifier.
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
        initialize_test_environment(&TEST_ENVIRONMENT_3);

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

        let mut seeds = HashMap::new();
        // Iterate over all chunk IDs.
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                let contributor = &contributors[contribution_id as usize - 1].clone();
                trace!("{} is processing contribution {}", contributor, contribution_id);
                {
                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    // Run computation as contributor.
                    let contribute =
                        coordinator.run_computation(round_height, chunk_id, contribution_id, &contributor, &seed);
                    if contribute.is_err() {
                        error!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(chunk_id, &contributor);
                    if contribute.is_err() {
                        error!(
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
                        error!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }

                    // Run verification as verifier.
                    let verify = coordinator.run_verification(round_height, chunk_id, contribution_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(chunk_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
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
        initialize_test_environment(&TEST_ENVIRONMENT_3);

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
        let mut seeds = HashMap::new();
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // Ensure contribution ID 0 is already verified by the coordinator.
            assert!(
                coordinator
                    .current_round()?
                    .chunk(chunk_id)?
                    .get_contribution(0)?
                    .is_verified()
            );

            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                {
                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }

                    // Run computation as contributor.
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    let contribute =
                        coordinator.run_computation(round_height, chunk_id, contribution_id, &contributor, &seed);
                    if contribute.is_err() {
                        error!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(chunk_id, &contributor);
                    if contribute.is_err() {
                        error!(
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
                        error!(
                            "Failed to acquire lock as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }

                    // Run verification as verifier.
                    let verify = coordinator.run_verification(round_height, chunk_id, contribution_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(chunk_id, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }
                }
            }
        }

        info!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        // Run aggregation and transition from round 1 to round 2.
        coordinator.next_round(Utc::now(), vec![contributor.clone()], vec![verifier.clone()])?;

        info!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_initialization_matches_json() {
        test_report!(coordinator_initialization_matches_json_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_initialization() {
        test_report!(coordinator_initialization_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_contributor_try_lock() {
        test_report!(coordinator_contributor_try_lock_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_contributor_add_contribution() {
        test_report!(coordinator_contributor_add_contribution_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_verifier_verify_contribution() {
        test_report!(coordinator_verifier_verify_contribution_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_concurrent_contribution_verification() {
        test_report!(coordinator_concurrent_contribution_verification_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_aggregation() {
        test_report!(coordinator_aggregation_test);
    }

    #[test]
    #[named]
    #[serial]
    fn test_coordinator_next_round() {
        test_report!(coordinator_next_round_test);
    }

    #[test]
    #[serial]
    #[ignore]
    fn test_coordinator_number_of_chunks() {
        let environment = &*TestingEnvironment::from(Parameters::TestChunks(4096));
        initialize_test_environment(environment);

        let coordinator = Coordinator::new(environment.clone()).unwrap();
        initialize_coordinator(&coordinator).unwrap();

        assert_eq!(
            environment.number_of_chunks(),
            coordinator.get_round(0).unwrap().chunks().len() as u64
        );
    }
}
