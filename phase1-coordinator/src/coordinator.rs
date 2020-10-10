#[cfg(test)]
use crate::commands::Computation;
use crate::{
    commands::{Aggregation, Initialization, Verification},
    environment::Environment,
    objects::{Participant, Round},
    storage::{Locator, Object, Storage},
};
use setup_utils::calculate_hash;

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use std::{
    collections::{HashMap, LinkedList},
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
    ParticipantAlreadyAdded,
    ParticipantAlreadyDropped,
    ParticipantAlreadyFinished,
    ParticipantAlreadyStarted,
    ParticipantIsBanned,
    ParticipantHasNotStarted,
    ParticipantHasNoRemainingChunks,
    ParticipantHasRemainingChunks,
    ParticipantNotFound,
    ParticipantRoundHeightMismatch,
    ParticipantRoundHeightMissing,
    ParticipantStillHasLocks,
    ParticipantUnauthorized,
    ParticipantWasDropped,
    Phase1Setup(setup_utils::Error),
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
    StorageReaderFailed,
    StorageSizeLookupFailed,
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

#[derive(Debug, Clone)]
pub struct ParticipantInfo {
    /// The ID of the participant.
    id: Participant,
    /// The timestamp of the first seen instance of this participant.
    first_seen: DateTime<Utc>,
    /// The timestamp of the last seen instance of this participant.
    last_seen: DateTime<Utc>,
    /// The timestamp when this participant started the round.
    started_at: Option<DateTime<Utc>>,
    /// The timestamp when this participant finished the round.
    finished_at: Option<DateTime<Utc>>,
    /// The timestamp when this participant was dropped from the round.
    dropped_at: Option<DateTime<Utc>>,
    /// The round height that this participant is contributing to.
    round_height: Option<u64>,
    /// The list of chunk IDs that this participant has remaining to compute.
    remaining_chunks: Option<LinkedList<u64>>,
    /// The number of locks held by this participant in the round.
    number_of_locks_held: u64,
}

impl ParticipantInfo {
    #[inline]
    pub fn new(participant: Participant, round_height: u64) -> Self {
        // Fetch the current time.
        let now = Utc::now();
        Self {
            id: participant,
            first_seen: now,
            last_seen: now,
            started_at: None,
            finished_at: None,
            dropped_at: None,
            round_height: Some(round_height),
            remaining_chunks: None,
            number_of_locks_held: 0,
        }
    }

    ///
    /// Returns `true` if the participant is finished with the current round.
    ///
    #[inline]
    pub fn is_finished(&self) -> bool {
        // Check that the participant already started in the round.
        if self.started_at.is_none() {
            return false;
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return false;
        }

        // Check that the participant has already finished the round.
        if self.finished_at.is_none() {
            return false;
        }

        // Check that the participant has no more remaining chunks.
        if self.remaining_chunks.is_some() {
            return false;
        }

        // Check that the participant has released all locks.
        if self.number_of_locks_held > 0 {
            return false;
        }

        true
    }

    ///
    /// Assigns the participant to the given chunks for the current round,
    /// and sets the start time as the current time.
    ///
    #[inline]
    pub fn start(&mut self, round_height: u64, chunks: LinkedList<u64>) -> Result<(), CoordinatorError> {
        // Check that the participant has not already started in the round.
        if self.started_at.is_some() || self.dropped_at.is_some() || self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant has a round height set.
        if self.round_height.is_none() {
            return Err(CoordinatorError::ParticipantRoundHeightMissing);
        }

        // Check that the participant has no remaining chunks.
        if self.remaining_chunks.is_some() {
            return Err(CoordinatorError::ParticipantHasRemainingChunks);
        }

        // Check that the round height matches the one set in the participant info.
        if self.round_height != Some(round_height) {
            return Err(CoordinatorError::ParticipantRoundHeightMismatch);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them starting now.
        self.last_seen = now;
        self.started_at = Some(now);
        self.remaining_chunks = Some(chunks);

        Ok(())
    }

    ///
    /// Returns the next chunk ID the participant should process,
    /// in FIFO order when added to the linked list.
    ///
    #[inline]
    pub fn next_chunk_id(&mut self) -> Result<u64, CoordinatorError> {
        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Fetch the next chunk ID in order as stored.
        match &mut self.remaining_chunks {
            Some(remaining) => {
                // If the participant has no more remaining chunks, set the remaining chunks to `None`.
                if remaining.is_empty() {
                    self.remaining_chunks = None;
                    return Err(CoordinatorError::ParticipantHasNoRemainingChunks);
                }

                match remaining.pop_front() {
                    Some(chunk_id) => Ok(chunk_id),
                    None => Err(CoordinatorError::ParticipantHasNoRemainingChunks),
                }
            }
            None => Err(CoordinatorError::ParticipantHasNotStarted),
        }
    }

    ///
    /// Increments the number of locks held by this participant by 1.
    ///
    #[inline]
    pub fn acquired_lock(&mut self) -> Result<(), CoordinatorError> {
        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Increment the number of locks held by 1.
        self.number_of_locks_held += 1;

        Ok(())
    }

    ///
    /// Decrements the number of locks held by this participant by 1.
    ///
    #[inline]
    pub fn released_lock(&mut self) -> Result<(), CoordinatorError> {
        // Check that the participant has started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Decrement the number of locks held by 1.
        self.number_of_locks_held -= 1;

        Ok(())
    }

    ///
    /// Sets the participant to finished and saves the current time as the completed time.
    ///
    #[inline]
    pub fn finish(&mut self) -> Result<(), CoordinatorError> {
        // Check that the participant already started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantWasDropped);
        }

        // Check that the participant has not already finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Check that the participant has no more remaining chunks.
        if self.remaining_chunks.is_some() {
            return Err(CoordinatorError::ParticipantHasRemainingChunks);
        }

        // Check that the participant has released all locks.
        if self.number_of_locks_held > 0 {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them finishing now.
        self.last_seen = now;
        self.finished_at = Some(now);

        Ok(())
    }

    ///
    /// Sets the participant to dropped and saves the current time as the dropped time.
    ///
    #[inline]
    pub fn drop(&mut self) -> Result<(), CoordinatorError> {
        // Check that the participant already started in the round.
        if self.started_at.is_none() {
            return Err(CoordinatorError::ParticipantHasNotStarted);
        }

        // Check that the participant was not already dropped from the round.
        if self.dropped_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyDropped);
        }

        // Check that the participant has not already finished the round.
        if self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyFinished);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them dropping now.
        self.dropped_at = Some(now);

        Ok(())
    }
}

pub struct CoordinatorState {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The set of unique participants for the current round.
    current: HashMap<Participant, ParticipantInfo>,
    /// The set of unique participants for the next round.
    next: HashMap<Participant, ParticipantInfo>,
    /// The list of information about participants that finished in current and past rounds.
    finished: Vec<ParticipantInfo>,
    /// The list of information about participants that dropped in current and past rounds.
    dropped: Vec<ParticipantInfo>,
    /// The list of participants that are banned from all current and future rounds.
    banned: Vec<Participant>,
}

impl CoordinatorState {
    ///
    /// Adds the given participant to the next round if they are permitted to participate.
    /// On success, returns `true`. Otherwise, returns `false`.
    ///
    #[inline]
    pub fn add_participant_to_next_round(
        &mut self,
        participant: Participant,
        round_height: u64,
    ) -> Result<(), CoordinatorError> {
        // Check if the participant is banned.
        if self.banned.contains(&participant) {
            return Err(CoordinatorError::ParticipantIsBanned);
        }

        // Check that the participant is not already added to the next round.
        if self.next.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyAdded);
        }

        // Check if there is room to add the participant to the next round.
        match participant {
            Participant::Contributor(_) => {
                if self
                    .next
                    .par_iter()
                    .filter(|(participant, _)| participant.is_contributor())
                    .count()
                    < self.environment.number_of_contributors_per_round()
                {
                    // Add the participant to the next round.
                    self.next
                        .insert(participant.clone(), ParticipantInfo::new(participant, round_height));

                    return Ok(());
                }
            }
            Participant::Verifier(_) => {
                if self
                    .next
                    .par_iter()
                    .filter(|(participant, _)| participant.is_verifier())
                    .count()
                    < self.environment.number_of_verifiers_per_round()
                {
                    // Add the participant to the next round.
                    self.next
                        .insert(participant.clone(), ParticipantInfo::new(participant, round_height));

                    return Ok(());
                }
            }
        }

        Err(CoordinatorError::ParticipantUnauthorized)
    }

    ///
    /// Returns `true` if all participants in the current round have no more remaining chunks.
    ///
    #[inline]
    pub fn is_current_round_finished(&self) -> bool {
        self.current.is_empty()
    }

    ///
    /// Adds the given participant to the next round if they are permitted to participate.
    /// On success, returns `true`. Otherwise, returns `false`.
    ///
    #[inline]
    pub fn next_chunk_id(&mut self, participant: &Participant) -> Result<u64, CoordinatorError> {
        match self.current.get_mut(participant) {
            Some(participant) => Ok(participant.next_chunk_id()?),
            None => Err(CoordinatorError::ParticipantNotFound),
        }
    }

    ///
    /// Checks the current round for finished participants.
    ///
    #[inline]
    pub fn update_finished_participants(&mut self) -> Result<(), CoordinatorError> {
        // Split the current participant into (finished, current).
        let (finished, current): (HashMap<_, _>, HashMap<_, _>) = self
            .current
            .par_iter()
            .partition(|(_, participant_info)| participant_info.is_finished());

        // Add the finished participants into the finished list.
        for (_, participant_info) in finished.into_iter() {
            // Clone the participant info.
            let mut finished_info = participant_info.clone();

            // Set the participant as finished.
            finished_info.finish()?;

            self.finished.push(finished_info);
        }

        // Set current to the updated map.
        let mut updated = HashMap::default();
        for (participant, participant_info) in current.into_iter() {
            updated.insert(participant.clone(), participant_info.clone());
        }

        self.current = updated;

        Ok(())
    }

    ///
    /// Checks the current round for disconnected participants.
    ///
    #[inline]
    pub fn update_dropped_participants(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the timeout threshold for contributors and verifiers.
        let contributor_timeout = self.environment.contributor_timeout_in_minutes() as i64;
        let verifier_timeout = self.environment.verifier_timeout_in_minutes() as i64;

        // Fetch the current time.
        let now = Utc::now();

        for (participant, mut participant_info) in self.current.clone() {
            // Fetch the elapsed time.
            let elapsed = now - participant_info.last_seen;

            match participant {
                Participant::Contributor(_) => {
                    // Check if the participant is still live.
                    if elapsed.num_minutes() > contributor_timeout {
                        // Set the participant as dropped.
                        participant_info.drop()?;

                        self.dropped.push(participant_info);
                        self.current.remove(&participant);
                    }
                }
                Participant::Verifier(_) => {
                    // Check if the participant is still live.
                    if elapsed.num_minutes() > verifier_timeout {
                        // Set the participant as dropped.
                        participant_info.drop()?;

                        // TODO (howardwu): Release any outstanding locks.

                        self.dropped.push(participant_info);
                        self.current.remove(&participant);
                    }
                }
            }
        }

        Ok(())
    }

    ///
    /// Checks the list of dropped participants for participants who
    /// meet the ban criteria of the coordinator.
    ///
    #[inline]
    pub fn update_banned_participants(&mut self) -> Result<(), CoordinatorError> {
        for participant_info in self.dropped.iter() {
            // Fetch the number of times this participant has been dropped.
            let count = self
                .dropped
                .par_iter()
                .filter(|dropped| dropped.id == participant_info.id)
                .count();

            // Check if the participant meets the ban threshold.
            if count > self.environment.participant_ban_threshold() as usize {
                self.banned.push(participant_info.id.clone());
            }
        }

        Ok(())
    }
}

/// A core structure for operating the Phase 1 ceremony.
pub struct Coordinator {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The storage of contributions and rounds for this coordinator.
    storage: Arc<RwLock<Box<dyn Storage>>>,
    /// The current round and participant state.
    state: CoordinatorState,
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
            state: CoordinatorState {
                environment,
                current: HashMap::default(),
                next: HashMap::default(),
                finished: Vec::new(),
                dropped: Vec::new(),
                banned: Vec::new(),
            },
        })
    }

    ///
    /// Runs a set of operations to update the coordinator state to reflect
    /// newly finished, dropped, or banned participants.
    ///
    #[inline]
    pub async fn update(&mut self) -> Result<(), CoordinatorError> {
        // Check the current round for finished participants.
        self.state.update_finished_participants()?;

        // Drop disconnected participants from the current round.
        self.state.update_dropped_participants()?;

        // Ban any participants who meet the coordinator criteria.
        self.state.update_banned_participants()?;

        // TODO (howardwu): Check to start next round.

        Ok(())
    }

    ///
    /// Adds the given participant to the next round if they are a contributor,
    /// and permitted to participate. On success, returns `true`. Otherwise, returns `false`.
    ///
    #[inline]
    pub fn add_contributor_to_next_round(&mut self, participant: Participant) -> Result<(), CoordinatorError> {
        // Fetch the next round height.
        let next_round_height = self.current_round_height()? + 1;

        // Attempt to add the participant to the next round.
        self.state.add_participant_to_next_round(participant, next_round_height)
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
    /// Attempts to acquire the lock for a given participant.
    ///
    /// On success, this function returns the next contribution locator
    /// if the participant is a contributor, and it returns the current
    /// contribution locator if the participant is a verifier.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn try_lock(&mut self, participant: &Participant) -> Result<String, CoordinatorError> {
        // Attempt to fetch the next chunk ID for the given participant.
        let chunk_id = self.state.next_chunk_id(participant)?;

        info!("Trying to lock chunk {} for {}", chunk_id, participant);
        self.try_lock_chunk(chunk_id, participant)
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
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID of the chunk.
        let current_contribution_id = chunk.current_contribution_id();
        // Fetch the next contribution ID of the chunk.
        let next_contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the next contribution ID is one above the current contribution ID.
        if !chunk.is_next_contribution_id(next_contribution_id, expected_num_contributions) {
            return Err(CoordinatorError::ContributionIdMismatch);
        }

        // Fetch the challenge and response locators.
        let challenge_file_locator =
            Locator::ContributionFile(current_round_height, chunk_id, current_contribution_id, true);
        let response_file_locator =
            Locator::ContributionFile(current_round_height, chunk_id, next_contribution_id, false);

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
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();
        // Fetch the next contribution ID.
        let current_contribution = chunk.current_contribution()?;

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
            let round = match current_round_height != 0 {
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
    /// Attempts to acquire the lock for a given chunk ID and participant.
    ///
    /// On success, this function returns the next contribution locator
    /// if the participant is a contributor, and it returns the current
    /// contribution locator if the participant is a verifier.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn try_lock_chunk(&self, chunk_id: u64, participant: &Participant) -> Result<String, CoordinatorError> {
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
                info!("{} acquired lock on chunk {}", participant, chunk_id);
                storage.to_path(&contribution_locator)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Returns a reference to the instantiation of `Environment` that this
    /// coordinator is using.
    ///
    #[allow(dead_code)]
    #[cfg(test)]
    #[inline]
    pub(crate) fn environment(&self) -> &Environment {
        &self.environment
    }

    ///
    /// Returns a reference to the instantiation of `Storage` that this
    /// coordinator is using.
    ///
    #[cfg(test)]
    #[inline]
    pub(crate) fn storage(&self) -> Arc<RwLock<Box<dyn Storage>>> {
        self.storage.clone()
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
    pub(crate) fn run_computation(
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
        let mut storage = self.storage.write().unwrap();

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
        let chunk = round.chunk(chunk_id)?;
        if chunk.get_contribution(contribution_id).is_ok() {
            return Err(CoordinatorError::ContributionShouldNotExist);
        }

        trace!(
            "Contributor starting computation on round {} chunk {} contribution {}",
            round_height,
            chunk_id,
            contribution_id
        );
        Computation::run(&self.environment, &mut storage, round_height, chunk_id, contribution_id)?;
        trace!(
            "Contributor completed computation on round {} chunk {} contribution {}",
            round_height,
            chunk_id,
            contribution_id
        );

        info!("Computed chunk {} contribution {}", chunk_id, contribution_id);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{environment::*, testing::prelude::*, Coordinator};

    use chrono::Utc;
    use once_cell::sync::Lazy;
    use std::{panic, process};
    use tracing::debug;

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
        initialize_test_environment();

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
        initialize_test_environment();

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
        initialize_test_environment();

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
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_unlocked());
            assert!(!chunk.is_locked());
        }

        Ok(())
    }

    fn coordinator_verifier_verify_contribution_test() -> anyhow::Result<()> {
        initialize_test_environment();

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
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by the verifier.
            debug!("{:#?}", round);
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
        initialize_test_environment();

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
        initialize_test_environment();

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
        initialize_test_environment();

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
                    .chunk(chunk_id)?
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
        initialize_test_environment();

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
    fn test_coordinator_verifier_verify_contribution() {
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
    #[ignore]
    fn test_coordinator_number_of_chunks() {
        initialize_test_environment();

        let environment = Environment::Test(Parameters::AleoTestChunks(4096));

        let coordinator = Coordinator::new(environment.clone()).unwrap();
        initialize_coordinator(&coordinator).unwrap();

        assert_eq!(
            environment.number_of_chunks(),
            coordinator.get_round(0).unwrap().chunks().len() as u64
        );
    }
}
