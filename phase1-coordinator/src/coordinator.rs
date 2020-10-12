use crate::{
    commands::{Aggregation, Computation, Initialization, Verification},
    environment::Environment,
    objects::{participant::*, Round},
    storage::{Locator, Object, Storage, StorageLock},
};
use setup_utils::calculate_hash;

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, LinkedList},
    fmt,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{task, time::delay_for};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub enum CoordinatorError {
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
    LocatorSerializationFailed,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParticipantInfo {
    /// The ID of the participant.
    #[serde(
        serialize_with = "serialize_participant_to_string",
        deserialize_with = "deserialize_participant_to_string"
    )]
    id: Participant,
    /// The round height that this participant is contributing to.
    round_height: u64,
    /// The reliability of the participant from an initial calibration.
    reliability: u8,
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
    /// The list of chunk IDs that this participant has left to compute.
    pending_chunks: LinkedList<u64>,
    /// The set of chunk IDs that this participant is computing.
    locked_chunks: HashSet<u64>,
    /// The list of chunk IDs that this participant has computed.
    completed_chunks: LinkedList<u64>,
}

impl ParticipantInfo {
    #[inline]
    fn new(participant: Participant, round_height: u64, reliability: u8) -> Self {
        // Fetch the current time.
        let now = Utc::now();
        Self {
            id: participant,
            round_height,
            reliability,
            first_seen: now,
            last_seen: now,
            started_at: None,
            finished_at: None,
            dropped_at: None,
            pending_chunks: LinkedList::new(),
            locked_chunks: HashSet::new(),
            completed_chunks: LinkedList::new(),
        }
    }

    ///
    /// Returns `true` if the participant is finished with the current round.
    ///
    #[inline]
    fn is_finished(&self) -> bool {
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

        // Check that the participant has no more pending chunks.
        if !self.pending_chunks.is_empty() {
            return false;
        }

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return false;
        }

        // Check that the participant has completed chunks.
        if self.completed_chunks.is_empty() {
            return false;
        }

        true
    }

    ///
    /// Assigns the participant to the given chunks for the current round,
    /// and sets the start time as the current time.
    ///
    #[inline]
    fn start(&mut self, chunks: LinkedList<u64>) -> Result<(), CoordinatorError> {
        // Check that the participant has a valid round height set.
        if self.round_height == 0 {
            return Err(CoordinatorError::ParticipantRoundHeightInvalid);
        }

        // Check that the participant has not already started in the round.
        if self.started_at.is_some() || self.dropped_at.is_some() || self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant has no pending or locked chunks.
        if !self.pending_chunks.is_empty() || !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingChunks);
        }

        // Check that the participant has not completed chunks already.
        if !self.completed_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them starting now.
        self.last_seen = now;
        self.started_at = Some(now);
        self.pending_chunks = chunks;

        Ok(())
    }

    ///
    /// Adds the given chunk ID in FIFO order for the participant to process.
    ///
    #[inline]
    fn push_back_chunk_id(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
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

        // Check that the chunk ID was not already added to the pending chunks.
        if self.pending_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the chunk ID was not already locked or completed.
        if self.locked_chunks.contains(&chunk_id) || self.completed_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Add the chunk ID to the front of the linked list.
        self.pending_chunks.push_back(chunk_id);

        Ok(())
    }

    ///
    /// Adds the given chunk ID in LIFO order for the participant to process.
    ///
    #[inline]
    fn push_front_chunk_id(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
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

        // Check that the chunk ID was not already added to the pending chunks.
        if self.pending_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the chunk ID was not already locked or completed.
        if self.locked_chunks.contains(&chunk_id) || self.completed_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Add the chunk ID to the front of the linked list.
        self.pending_chunks.push_front(chunk_id);

        Ok(())
    }

    ///
    /// Pops the next chunk ID the participant should process,
    /// in FIFO order when added to the linked list.
    ///
    #[inline]
    fn pop_chunk_id(&mut self) -> Result<u64, CoordinatorError> {
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

        // Check that the participant has pending chunks.
        if self.pending_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantHasNoRemainingChunks);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Fetch the next chunk ID in order as stored.
        match self.pending_chunks.pop_front() {
            Some(chunk_id) => Ok(chunk_id),
            None => Err(CoordinatorError::ParticipantHasNoRemainingChunks),
        }
    }

    ///
    /// Adds the given chunk ID to the locked chunks held by this participant.
    ///
    #[inline]
    fn acquired_lock(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
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

        // Check that the participant was authorized to lock this chunk.
        if self.pending_chunks.contains(&chunk_id) || self.completed_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantUnauthorizedForChunkId);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Adds the given chunk ID to the locked chunks.
        self.locked_chunks.insert(chunk_id);

        Ok(())
    }

    ///
    /// Removes the given chunk ID from the locked chunks held by this participant.
    ///
    #[inline]
    fn released_lock(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
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

        // Check that the participant had locked this chunk.
        if self.pending_chunks.contains(&chunk_id) || !self.locked_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantDidntLockChunkId);
        }

        // Check that the participant has not already completed this chunk.
        if self.completed_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Remove the given chunk ID from the locked chunks.
        self.locked_chunks.remove(&chunk_id);

        // Adds the given chunk ID to the completed chunks.
        self.completed_chunks.push_back(chunk_id);

        Ok(())
    }

    ///
    /// Sets the participant to finished and saves the current time as the completed time.
    ///
    #[inline]
    fn finish(&mut self) -> Result<(), CoordinatorError> {
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

        // Check that the participant has no more pending chunks.
        if !self.pending_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingChunks);
        }

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Check that the participant has completed chunks.
        if self.completed_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantDidNotWork);
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
    fn drop(&mut self) -> Result<(), CoordinatorError> {
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

        // Check that the participant has no more pending chunks.
        if !self.pending_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingChunks);
        }

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them dropping now.
        self.dropped_at = Some(now);

        Ok(())
    }

    ///
    /// Resets the participant information.
    ///
    #[inline]
    fn reset(&mut self) {
        *self = Self::new(self.id.clone(), self.round_height, self.reliability);
    }
}

#[derive(Serialize)]
struct CoordinatorState {
    /// The parameters and settings of this coordinator.
    #[serde(skip_serializing)]
    environment: Environment,
    /// The map of queue participants with a reliability score and an assigned future round.
    queue: HashMap<Participant, (u8, Option<u64>)>,
    /// The map of unique participants for the next round.
    next: HashMap<Participant, ParticipantInfo>,
    /// The current round height of the ceremony.
    current_round_height: Option<u64>,
    /// The map of unique contributors for the current round.
    current_contributors: HashMap<Participant, ParticipantInfo>,
    /// The map of unique verifiers for the current round.
    current_verifiers: HashMap<Participant, ParticipantInfo>,
    /// The map of chunks pending verification in the current round.
    pending_verification: HashMap<u64, Participant>,
    /// The map of each round height to the corresponding contributors from that round.
    finished_contributors: HashMap<u64, ParticipantInfo>,
    /// The map of each round height to the corresponding verifiers from that round.
    finished_verifiers: HashMap<u64, ParticipantInfo>,
    /// The list of information about participants that dropped in current and past rounds.
    dropped: Vec<ParticipantInfo>,
    /// The list of participants that are banned from all current and future rounds.
    banned: Vec<Participant>,
}

impl CoordinatorState {
    ///
    /// Creates a new instance of `CoordinatorState`.
    ///
    #[inline]
    fn new(environment: Environment) -> Self {
        Self {
            environment,
            queue: HashMap::default(),
            next: HashMap::default(),
            current_round_height: None,
            current_contributors: HashMap::default(),
            current_verifiers: HashMap::default(),
            pending_verification: HashMap::default(),
            finished_contributors: HashMap::default(),
            finished_verifiers: HashMap::default(),
            dropped: Vec::new(),
            banned: Vec::new(),
        }
    }

    ///
    /// Returns `true` if the given participant is an authorized contributor in the ceremony.
    ///
    #[inline]
    fn is_authorized_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if the given participant is an authorized verifier in the ceremony.
    ///
    #[inline]
    fn is_authorized_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if all participants in the current round have no more pending chunks.
    ///
    #[inline]
    fn is_current_round_finished(&self) -> bool {
        // Check that all contributions have undergone verification.
        self.pending_verification.is_empty()
            // Check that all current contributors are finished.
            && (self
            .current_contributors
            .par_iter()
            .filter(|(_, p)| !p.is_finished())
            .count()
            == 0)
            // Check that all current verifiers are finished.
            && (self
            .current_verifiers
            .par_iter()
            .filter(|(_, p)| !p.is_finished())
            .count()
            == 0)
    }

    ///
    /// Returns `true` if the next round has the required number of contributors and verifiers.
    ///
    /// Note that this function does not check for banned participants, which is checked
    /// during the precommit phase for the next round.
    ///
    #[inline]
    fn is_next_round_ready(&self) -> bool {
        // Check that the next round contains participants.
        if self.next.is_empty() {
            return false;
        }

        // Parse the next round participants into contributors and verifiers.
        let contributors = self.next.par_iter().filter(|(p, _)| p.is_contributor());
        let verifiers = self.next.par_iter().filter(|(p, _)| p.is_verifier());

        // Check that the next round contains a permitted number of contributors.
        let minimum_contributors = self.environment.minimum_contributors_per_round();
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let number_of_contributors = contributors.count();
        if number_of_contributors < minimum_contributors || number_of_contributors > maximum_contributors {
            trace!("Insufficient or unauthorized number of contributors");
            return false;
        }

        // Check that the next round contains a permitted number of verifiers.
        let minimum_verifiers = self.environment.minimum_verifiers_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();
        let number_of_verifiers = verifiers.count();
        if number_of_verifiers < minimum_verifiers || number_of_verifiers > maximum_verifiers {
            trace!("Insufficient or unauthorized number of verifiers");
            return false;
        }

        true
    }

    ///
    /// Updates the current round height stored in the coordinator state.
    ///
    #[inline]
    fn set_current_round_height(&mut self, current_round_height: u64) {
        self.current_round_height = Some(current_round_height);
    }

    ///
    /// Adds the given chunk ID to the participant to process.
    ///
    #[inline]
    fn push_front_chunk_id(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Add the chunk ID to the pending chunks of the given participant.
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                Some(participant) => Ok(participant.push_front_chunk_id(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                Some(participant) => Ok(participant.push_front_chunk_id(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Pops the next chunk ID the participant should process.
    ///
    #[inline]
    fn pop_chunk_id(&mut self, participant: &Participant) -> Result<u64, CoordinatorError> {
        // Remove the next chunk ID from the pending chunks of the given participant.
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                Some(participant) => Ok(participant.pop_chunk_id()?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                Some(participant) => Ok(participant.pop_chunk_id()?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Adds the given chunk ID to the locks held by the given participant.
    ///
    #[inline]
    fn acquired_lock(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                Some(participant) => Ok(participant.acquired_lock(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                Some(participant) => Ok(participant.acquired_lock(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Removes the given chunk ID from the locks held by the given participant.
    ///
    #[inline]
    fn released_lock(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                Some(participant_info) => {
                    // Release the chunk lock from the contributor.
                    participant_info.released_lock(chunk_id)?;

                    trace!("Adding chunk {} to the queue of pending verifications", chunk_id);
                    self.add_pending_verification(chunk_id)?;

                    Ok(())
                }
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                Some(participant) => {
                    // Release the chunk lock from the verifier.
                    participant.released_lock(chunk_id)?;

                    trace!("Removing chunk {} from the queue of pending verifications", chunk_id);
                    self.remove_pending_verification(chunk_id)?;

                    Ok(())
                }
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Adds the given chunk ID to the map of chunks that are pending verification.
    /// The chunk is assigned to the verifier with the least number of chunks in its queue.
    ///
    #[inline]
    fn add_pending_verification(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the set pending verification does not already contain the chunk ID.
        if self.pending_verification.contains_key(&chunk_id) {
            return Err(CoordinatorError::ChunkIdAlreadyAdded);
        }

        let verifier = match self
            .current_verifiers
            .par_iter()
            .min_by_key(|(_, v)| v.pending_chunks.len() + v.locked_chunks.len())
        {
            Some((verifier, verifier_info)) => verifier.clone(),
            None => return Err(CoordinatorError::VerifierMissing),
        };

        trace!("Assigning chunk {} to {} for verification", chunk_id, verifier);

        match self.current_verifiers.get_mut(&verifier) {
            Some(verifier_info) => verifier_info.push_back_chunk_id(chunk_id)?,
            None => return Err(CoordinatorError::VerifierMissing),
        };

        self.pending_verification.insert(chunk_id, verifier);

        Ok(())
    }

    ///
    /// Remove the given chunk ID from the map of chunks that are pending verification.
    ///
    #[inline]
    fn remove_pending_verification(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the set pending verification does not already contain the chunk ID.
        if !self.pending_verification.contains_key(&chunk_id) {
            return Err(CoordinatorError::ChunkIdMissing);
        }

        trace!("Removing chunk {} from the pending verifications", chunk_id);
        self.pending_verification.remove(&chunk_id);

        Ok(())
    }

    ///
    /// Adds the given participant to the next round if they are permitted to participate.
    ///
    #[inline]
    fn add_to_next_round(
        &mut self,
        participant: Participant,
        round_height: u64,
        reliability_score: u8,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is not already added to the next round.
        if self.next.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyAdded);
        }

        // Check that the given round height is one above the current round height.
        match self.current_round_height {
            Some(current_round_height) => {
                if round_height != current_round_height + 1 {
                    error!(
                        "Attempting to add a participant to a next round of {} when the coordinator is on round {}",
                        round_height, current_round_height
                    );
                    return Err(CoordinatorError::RoundHeightMismatch);
                }
            }
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        }

        // Check if there is space to add the given participant to the next round.
        match participant {
            Participant::Contributor(_) => {
                // Check if the contributor is authorized.
                if !self.is_authorized_contributor(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Filter the next round participants into contributors.
                let num_contributors = self.next.par_iter().filter(|(p, _)| p.is_contributor()).count();

                if num_contributors < self.environment.maximum_contributors_per_round() {
                    // Add the contributor to the next round.
                    self.next.insert(
                        participant.clone(),
                        ParticipantInfo::new(participant, round_height, reliability_score),
                    );
                    return Ok(());
                }
            }
            Participant::Verifier(_) => {
                // Check if the verifier is authorized.
                if !self.is_authorized_verifier(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Filter the next round participants into verifiers.
                let num_verifiers = self.next.par_iter().filter(|(p, _)| p.is_verifier()).count();

                if num_verifiers < self.environment.maximum_verifiers_per_round() {
                    // Add the verifier to the next round.
                    self.next.insert(
                        participant.clone(),
                        ParticipantInfo::new(participant, round_height, reliability_score),
                    );
                    return Ok(());
                }
            }
        }

        Err(CoordinatorError::ParticipantUnauthorized)
    }

    ///
    /// Removes the given participant from the next round.
    ///
    #[inline]
    fn remove_from_next_round(&mut self, participant: Participant) -> Result<(), CoordinatorError> {
        // Remove the participant from the next round.
        match self.next.remove(&participant) {
            Some(participant_info) => {
                // Add them back into the queue.
                if let Ok(_) = self.add_to_queue(participant_info.id, participant_info.reliability) {
                    trace!("Added {} back into the queue", participant);
                }
                Ok(())
            }
            _ => Err(CoordinatorError::ParticipantMissing),
        }
    }

    ///
    /// Adds the given participant to the queue if they are permitted to participate.
    ///
    #[inline]
    fn add_to_queue(&mut self, participant: Participant, reliability_score: u8) -> Result<(), CoordinatorError> {
        // Check that the participant is not already added to the queue.
        if self.queue.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantAlreadyAdded);
        }

        // Check that the participant is not banned from participating.
        if self.banned.contains(&participant) {
            return Err(CoordinatorError::ParticipantBanned);
        }

        match &participant {
            Participant::Contributor(_) => {
                // Check if the contributor is authorized.
                if !self.is_authorized_contributor(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Add the contributor to the queue.
                self.queue.insert(participant, (reliability_score, None));
            }
            Participant::Verifier(_) => {
                // Check if the verifier is authorized.
                if !self.is_authorized_verifier(&participant) {
                    return Err(CoordinatorError::ParticipantUnauthorized);
                }

                // Add the contributor to the queue.
                self.queue.insert(participant, (reliability_score, None));
            }
        }

        Ok(())
    }

    ///
    /// Removes the given participant from the queue.
    ///
    #[inline]
    fn remove_from_queue(&mut self, participant: Participant) -> Result<(), CoordinatorError> {
        // Check that the participant is exists in the queue.
        if !self.queue.contains_key(&participant) {
            return Err(CoordinatorError::ParticipantMissing);
        }

        // Remove the participant from the queue.
        self.queue.remove(&participant);

        Ok(())
    }

    ///
    /// Updates the state of the queue for all waiting participants.
    ///
    #[inline]
    fn update_queue(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the next round height.
        let next_round = match self.current_round_height {
            Some(round_height) => round_height + 1,
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        };

        // Sort the participants in the queue by reliability.
        let mut queue: Vec<_> = self.queue.clone().into_par_iter().map(|(p, (r, _))| (p, r)).collect();
        queue.par_sort_by_key(|p| p.1);

        // Parse the queue participants into contributors and verifiers.
        let contributors: Vec<(_, _)> = queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_contributor())
            .collect();
        let verifiers: Vec<(_, _)> = queue.into_par_iter().filter(|(p, _)| p.is_verifier()).collect();

        // Fetch the permitted number of contributors and verifiers.
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();

        // Initialize the updated queue.
        let mut updated_queue = HashMap::with_capacity(contributors.len() + verifiers.len());

        // Update assigned round height for each contributor.
        for (index, round) in contributors.chunks(maximum_contributors).enumerate() {
            for (contributor, reliability) in round.into_iter() {
                updated_queue.insert(contributor.clone(), (*reliability, Some(next_round + index as u64)));
            }
        }

        // Update assigned round height for each contributor.
        for (index, round) in verifiers.chunks(maximum_verifiers).enumerate() {
            for (verifier, reliability) in round.into_iter() {
                updated_queue.insert(verifier.clone(), (*reliability, Some(next_round + index as u64)));
            }
        }

        // Set the queue to the updated queue.
        self.queue = updated_queue;

        Ok(())
    }

    ///
    /// Updates the state of contributors in the current round.
    ///
    #[inline]
    fn update_current_contributors(&mut self) -> Result<(), CoordinatorError> {
        for (contributor, contributor_info) in self.current_contributors.iter_mut() {
            trace!("{:#?}", contributor_info);

            // Check that the contributor already started in the round.
            if contributor_info.started_at.is_none() {
                continue;
            }

            // Check that the contributor was not dropped from the round.
            if contributor_info.dropped_at.is_some() {
                continue;
            }

            // Check that the contributor has not already finished the round.
            if contributor_info.finished_at.is_some() {
                continue;
            }

            // Check that the contributor has no more pending chunks.
            if !contributor_info.pending_chunks.is_empty() {
                continue;
            }

            // Check that the contributor has no more locked chunks.
            if !contributor_info.locked_chunks.is_empty() {
                continue;
            }

            // Check that the contributor has completed chunks.
            if contributor_info.completed_chunks.is_empty() {
                continue;
            }

            // Set the contributor as finished.
            contributor_info.finish()?;

            debug!("{} has finished", contributor);
        }
        Ok(())
    }

    ///
    /// Updates the state of verifiers in the current round.
    ///
    /// This function never be run prior to calling `update_current_contributors`.
    ///
    #[inline]
    fn update_current_verifiers(&mut self) -> Result<(), CoordinatorError> {
        // Check if the contributors are finished.
        let is_contributors_finished = self
            .current_contributors
            .par_iter()
            .filter(|(_, p)| !p.is_finished())
            .count()
            == 0;

        // If all contributors are finished, this means there are no new verification jobs
        // to be added to the pending verifications queue. So if a verifier is finished
        // with their verifications, then they are finished for this round.
        if is_contributors_finished {
            for (verifier, verifier_info) in self.current_verifiers.iter_mut() {
                trace!("{:#?}", verifier_info);

                // Check that the verifier already started in the round.
                if verifier_info.started_at.is_none() {
                    continue;
                }

                // Check that the verifier was not dropped from the round.
                if verifier_info.dropped_at.is_some() {
                    continue;
                }

                // Check that the verifier has not already finished the round.
                if verifier_info.finished_at.is_some() {
                    continue;
                }

                // Check that the verifier has no more pending chunks.
                if !verifier_info.pending_chunks.is_empty() {
                    continue;
                }

                // Check that the verifier has no more locked chunks.
                if !verifier_info.locked_chunks.is_empty() {
                    continue;
                }

                // Check that the verifier has completed chunks.
                if verifier_info.completed_chunks.is_empty() {
                    continue;
                }

                // Set the verifier as finished.
                verifier_info.finish()?;

                debug!("{} has finished", verifier);
            }
        }

        Ok(())
    }

    ///
    /// Checks the current round for disconnected participants.
    ///
    #[inline]
    fn update_dropped_participants(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the timeout threshold for contributors and verifiers.
        let contributor_timeout = self.environment.contributor_timeout_in_minutes() as i64;
        let verifier_timeout = self.environment.verifier_timeout_in_minutes() as i64;

        // Fetch the current time.
        let now = Utc::now();

        // Process the contributors.
        for (participant, mut participant_info) in self.current_contributors.clone() {
            // Fetch the elapsed time.
            let elapsed = now - participant_info.last_seen;

            // Check if the participant is still live.
            if elapsed.num_minutes() > contributor_timeout {
                // Set the participant as dropped.
                participant_info.drop()?;

                // TODO (howardwu): Release any outstanding locks.

                self.dropped.push(participant_info);
                self.current_contributors.remove(&participant);

                debug!("{} is being dropped", participant);
            }
        }

        // Process the verifiers.
        for (participant, mut participant_info) in self.current_verifiers.clone() {
            // Fetch the elapsed time.
            let elapsed = now - participant_info.last_seen;

            // Check if the participant is still live.
            if elapsed.num_minutes() > verifier_timeout {
                // Set the participant as dropped.
                participant_info.drop()?;

                // TODO (howardwu): Release any outstanding locks.

                self.dropped.push(participant_info);
                self.current_verifiers.remove(&participant);

                debug!("{} is being dropped", participant);
            }
        }

        Ok(())
    }

    ///
    /// Checks the list of dropped participants for participants who
    /// meet the ban criteria of the coordinator.
    ///
    #[inline]
    fn update_banned_participants(&mut self) -> Result<(), CoordinatorError> {
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

                debug!("{} is being banned", participant_info.id);
            }
        }

        Ok(())
    }

    ///
    /// Prepares transition of the coordinator state from the current round to the next round.
    /// On precommit success, returns the list of contributors and verifiers for the next round.
    ///
    #[inline]
    fn precommit_next_round(
        &mut self,
        next_round_height: u64,
    ) -> Result<(Vec<Participant>, Vec<Participant>), CoordinatorError> {
        trace!("Attempting to run precommit for round {}", next_round_height);

        // Check that the given round height is correct.
        // Fetch the next round height.
        let current_round_height = match self.current_round_height {
            Some(current_round_height) => {
                if next_round_height != current_round_height + 1 {
                    error!(
                        "Attempting to precommit to round {} when the next round should be {}",
                        next_round_height,
                        current_round_height + 1
                    );
                    return Err(CoordinatorError::RoundHeightMismatch);
                }
                current_round_height
            }
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        };

        // Check that the queue contains participants.
        if self.queue.is_empty() {
            return Err(CoordinatorError::QueueIsEmpty);
        }

        // Check that the staging area for the next round is empty.
        if !self.next.is_empty() {
            return Err(CoordinatorError::NextRoundShouldBeEmpty);
        }

        // Check that the current round is complete.
        if !self.is_current_round_finished() {
            return Err(CoordinatorError::RoundNotComplete);
        }

        // Parse the queued participants for the next round and split into contributors and verifiers.
        let mut contributors: Vec<(_, (_, _))> = self
            .queue
            .clone()
            .into_par_iter()
            .map(|(p, (r, rh))| (p, (r, rh.unwrap_or_default())))
            .filter(|(p, (_, rh))| p.is_contributor() && *rh == next_round_height)
            .collect();
        let verifiers: Vec<(_, (_, _))> = self
            .queue
            .clone()
            .into_par_iter()
            .map(|(p, (r, rh))| (p, (r, rh.unwrap_or_default())))
            .filter(|(p, (_, rh))| p.is_verifier() && *rh == next_round_height)
            .collect();

        // Check that each participant in the next round is authorized.
        if contributors
            .par_iter()
            .filter(|(participant, _)| self.banned.contains(participant))
            .count()
            > 0
        {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }
        if verifiers
            .par_iter()
            .filter(|(participant, _)| self.banned.contains(participant))
            .count()
            > 0
        {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the next round contains a permitted number of contributors.
        let minimum_contributors = self.environment.minimum_contributors_per_round();
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let number_of_contributors = contributors.len();
        if number_of_contributors < minimum_contributors || number_of_contributors > maximum_contributors {
            warn!(
                "Precommit found {} contributors, but expected between {} and {} contributors",
                number_of_contributors, minimum_contributors, maximum_contributors
            );
            return Err(CoordinatorError::RoundNumberOfContributorsUnauthorized);
        }

        // Check that the next round contains a permitted number of verifiers.
        let minimum_verifiers = self.environment.minimum_verifiers_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();
        let number_of_verifiers = verifiers.len();
        if number_of_verifiers < minimum_verifiers || number_of_verifiers > maximum_verifiers {
            warn!(
                "Precommit found {} verifiers, but expected between {} and {} verifiers",
                number_of_verifiers, minimum_verifiers, maximum_verifiers
            );
            return Err(CoordinatorError::RoundNumberOfVerifiersUnauthorized);
        }

        // Initialize the next round contributors and verifiers to return.
        let mut next_contributors = Vec::with_capacity(number_of_contributors);
        let mut next_verifiers = Vec::with_capacity(number_of_verifiers);

        // Create the initial chunk locking sequence for each contributor.
        {
            // Sort the contributors by their reliability.
            contributors.par_sort_by_key(|p| (p.1).0);

            // Fetch the number of chunks and bucket size.
            let number_of_chunks = self.environment.number_of_chunks() as u64;
            let bucket_size = number_of_chunks / number_of_contributors as u64;

            // Set the chunk ID ordering for each contributor.
            for (index, (participant, (reliability, next_round))) in contributors.into_iter().enumerate() {
                // Determine the starting and ending indices.
                let start = index as u64 * bucket_size;
                let end = start + number_of_chunks;

                // Add the chunk ID in FIFO ordering.
                let mut chunk_ids = LinkedList::new();
                for index in start..end {
                    let chunk_id = index % number_of_chunks;
                    chunk_ids.push_back(chunk_id);
                }

                // Check that each participant is storing the correct round height.
                if next_round != next_round_height && next_round != current_round_height + 1 {
                    warn!("Contributor claims round is {}, not {}", next_round, next_round_height);
                    return Err(CoordinatorError::RoundHeightMismatch);
                }

                // Initialize the participant info for the contributor.
                let mut participant_info = ParticipantInfo::new(participant.clone(), next_round_height, reliability);
                participant_info.start(chunk_ids)?;

                // Check that the chunk IDs are set in the participant information.
                if participant_info.pending_chunks.is_empty() {
                    return Err(CoordinatorError::ParticipantNotReady);
                }

                // Add the contributor to staging for the next round.
                self.next.insert(participant.clone(), participant_info);

                // Remove the contributor from the queue.
                self.queue.remove(&participant);

                // Add the next round contributors to the return output.
                next_contributors.push(participant);
            }
        }

        // Initialize the participant info for each verifier.
        for (participant, (reliability, next_round)) in verifiers {
            // Check that each participant is storing the correct round height.
            if next_round != next_round_height && next_round != current_round_height + 1 {
                warn!("Verifier claims round is {}, not {}", next_round, next_round_height);
                return Err(CoordinatorError::RoundHeightMismatch);
            }

            // Initialize the participant info for the verifier.
            let mut participant_info = ParticipantInfo::new(participant.clone(), next_round_height, reliability);
            participant_info.start(LinkedList::new())?;

            // Add the verifier to staging for the next round.
            self.next.insert(participant.clone(), participant_info);

            // Remove the verifier from the queue.
            self.queue.remove(&participant);

            // Add the next round contributors to the return output.
            next_verifiers.push(participant);
        }

        Ok((next_contributors, next_verifiers))
    }

    ///
    /// Executes transition of the coordinator state from the current round to the next round.
    ///
    #[inline]
    fn commit_next_round(&mut self) {
        // Add all current round participants to the finished list.
        for (_, contributor_info) in self.current_contributors.clone() {
            self.finished_contributors
                .insert(contributor_info.round_height, contributor_info);
        }
        for (_, verifier_info) in self.current_verifiers.clone() {
            self.finished_verifiers
                .insert(verifier_info.round_height, verifier_info);
        }

        // Reset the current round map.
        self.current_contributors = HashMap::new();
        self.current_verifiers = HashMap::new();

        // Add all participants from next to current.
        for (participant, participant_info) in self.next.iter() {
            match participant {
                Participant::Contributor(_) => self
                    .current_contributors
                    .insert(participant.clone(), participant_info.clone()),
                Participant::Verifier(_) => self
                    .current_verifiers
                    .insert(participant.clone(), participant_info.clone()),
            };
        }

        // Reset the next round map.
        self.next = HashMap::new();
    }

    ///
    /// Rolls back the precommit of the coordinator state for transitioning to the next round.
    ///
    #[inline]
    fn rollback_next_round(&mut self) {
        // Add each participant back into the queue.
        for (participant, participant_info) in &self.next {
            self.queue.insert(
                participant.clone(),
                (participant_info.reliability, Some(participant_info.round_height)),
            );
        }

        // Reset the next round map.
        self.next = HashMap::new();
    }

    ///
    /// Returns the status of the coordinator state.
    ///
    #[inline]
    fn status_report(&self) -> String {
        let number_of_current_contributors = self.current_contributors.len();
        let number_of_current_verifiers = self.current_verifiers.len();
        let number_of_pending_verifications = self.pending_verification.len();

        let number_of_queued_contributors = self.queue.par_iter().filter(|(p, _)| p.is_contributor()).count();
        let number_of_queued_verifiers = self.queue.par_iter().filter(|(p, _)| p.is_verifier()).count();

        // Parse the queued participants for the next round and split into contributors and verifiers.
        let next_round_height = self.current_round_height.unwrap_or_default();
        let number_of_next_contributors = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();
        let number_of_next_verifiers = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();

        let number_of_dropped_participants = self.dropped.len();
        let number_of_banned_participants = self.banned.len();

        format!(
            r#"
    {} contributors and {} verifiers in the current round
    {} chunks are pending verification

    {} contributors and {} verifiers selected for the next round
    {} contributors and {} verifiers total are in the queue

    {} participants dropped
    {} participants banned        
        "#,
            number_of_current_contributors,
            number_of_current_verifiers,
            number_of_pending_verifications,
            number_of_next_contributors,
            number_of_next_verifiers,
            number_of_queued_contributors,
            number_of_queued_verifiers,
            number_of_dropped_participants,
            number_of_banned_participants
        )
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
        // Fetch the current round height from storage.
        let current_round_height = self.current_round_height()?;

        // Set the current round height for coordinator state.
        let mut state = self.state.write().unwrap();
        state.set_current_round_height(current_round_height);
        drop(state);

        // If this is a new ceremony, execute the first round to initialize the ceremony.
        if current_round_height == 0 {
            // Fetch the contributor and verifier of the coordinator.
            let contributor = self.environment.coordinator_contributor();
            let verifier = self.environment.coordinator_verifier();

            {
                // Acquire the state write lock.
                let mut state = self.state.write().unwrap();

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
            for chunk_id in 0..self.environment.number_of_chunks() {
                debug!("Computing contributions for round 1 chunk {}", chunk_id);
                let (_chunk_id, _previous_response_locator, _challenge_locator, response_locator) =
                    self.try_lock(&contributor)?;
                self.run_computation(1, chunk_id, 1, &contributor)?;
                let _response_locator = self.try_contribute(&contributor, &response_locator)?;
                debug!("Computed contributions for round 1 chunk {}", chunk_id);

                debug!("Running verification for round 1 chunk {}", chunk_id);
                let (_chunk_id, _challenge_locator, _response_locator, next_challenge_locator) =
                    self.try_lock(&verifier)?;
                let _next_challenge_locator = self.run_verification(1, chunk_id, 1, &verifier)?;
                let _empty = self.try_verify(&verifier, &next_challenge_locator)?;
                debug!("Running verification for round 1 chunk {}", chunk_id);
            }
            info!("Added contributions and verifications for round 1");
        }

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

            info!("Status Report\n\t{}", state.status_report());

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

            // Determine if current round is finished and next round is ready.
            (state.is_current_round_finished(), state.is_next_round_ready())
        };

        trace!("{} {}", is_current_round_finished, is_next_round_ready);

        // Check if the current round is finished and the next round is ready.
        if is_current_round_finished && is_next_round_ready {
            // Check that all locators exist.

            // Backup a copy of the current coordinator.

            // Attempt to advance to the next round.
            let next_round_height = self.try_advance()?;
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
    pub fn remove_from_queue(&self, participant: Participant) -> Result<(), CoordinatorError> {
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
            Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => {
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
            Locator::ContributionFile(round_height, chunk_id, contribution_id, verified) => {
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
    /// On success, this function copies the current contribution into the next transcript locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    #[inline]
    pub(crate) fn verify_contribution(&self, chunk_id: u64, participant: &Participant) -> Result<(), CoordinatorError> {
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
        let mut storage = StorageLock::Write(self.storage.write().unwrap());

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&storage)?;

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

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();

        // Fetch whether this is the final contribution of the specified chunk.
        let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());

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

        info!(
            "Starting computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Computation::run(&self.environment, &mut storage, round_height, chunk_id, contribution_id)?;
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
