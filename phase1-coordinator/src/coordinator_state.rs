use crate::{environment::Environment, objects::participant::*, CoordinatorError};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, LinkedList};
use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) enum CoordinatorStatus {
    Initializing,
    Initialized,
    Precommit,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ParticipantInfo {
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
    #[deprecated]
    #[allow(dead_code)]
    #[inline]
    fn reset(&mut self) {
        warn!("Resetting the state of participant {}", self.id);
        *self = Self::new(self.id.clone(), self.round_height, self.reliability);
    }
}

#[derive(Serialize)]
pub(super) struct CoordinatorState {
    /// The parameters and settings of this coordinator.
    #[serde(skip_serializing)]
    environment: Environment,
    /// The current status of the coordinator.
    status: CoordinatorStatus,
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
    pub(super) fn new(environment: Environment) -> Self {
        Self {
            environment,
            status: CoordinatorStatus::Initializing,
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
    pub(super) fn is_authorized_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if the given participant is an authorized verifier in the ceremony.
    ///
    #[inline]
    pub(super) fn is_authorized_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && !self.banned.contains(participant)
    }

    ///
    /// Returns `true` if all participants in the current round have no more pending chunks.
    ///
    #[inline]
    pub(super) fn is_current_round_finished(&self) -> bool {
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
    /// Returns `true` if the precommit for the next round is ready.
    ///
    /// This function checks that the requisite number of contributors and verifiers are
    /// assigned for the next round.
    ///
    /// Note that this function does not check for banned participants, which is checked
    /// during the precommit phase for the next round.
    ///
    #[inline]
    pub(super) fn is_precommit_next_round_ready(&self) -> bool {
        // Check that the coordinator is initialized and is not already in a precommit stage.
        if self.status == CoordinatorStatus::Initializing || self.status == CoordinatorStatus::Precommit {
            return false;
        }

        // Check that the queue contains participants.
        if self.queue.is_empty() {
            trace!("Queue is currently empty");
            return false;
        }

        // Check that the current round height is set.
        if self.current_round_height.is_none() {
            warn!("Current round height is not set in the coordinator state");
            return false;
        }

        // Fetch the next round height.
        let next_round_height = self.current_round_height.unwrap_or_default() + 1;

        // Fetch the state of assigned contributors for the next round in the queue.
        let minimum_contributors = self.environment.minimum_contributors_per_round();
        let maximum_contributors = self.environment.maximum_contributors_per_round();
        let number_of_assigned_contributors = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_contributor() && rh.unwrap_or_default() == next_round_height)
            .count();

        // Fetch the state of assigned verifiers for the next round in the queue.
        let minimum_verifiers = self.environment.minimum_verifiers_per_round();
        let maximum_verifiers = self.environment.maximum_verifiers_per_round();
        let number_of_assigned_verifiers = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();

        trace!(
            "Prepare precommit status - {} contributors assigned ({}-{} required), {} verifiers assigned ({}-{} required)",
            number_of_assigned_contributors,
            minimum_contributors,
            maximum_contributors,
            number_of_assigned_verifiers,
            minimum_verifiers,
            maximum_verifiers
        );

        // Check that the next round contains a permitted number of contributors.
        if number_of_assigned_contributors < minimum_contributors
            || number_of_assigned_contributors > maximum_contributors
        {
            trace!("Insufficient or unauthorized number of contributors");
            return false;
        }

        // Check that the next round contains a permitted number of verifiers.
        if number_of_assigned_verifiers < minimum_verifiers || number_of_assigned_verifiers > maximum_verifiers {
            trace!("Insufficient or unauthorized number of verifiers");
            return false;
        }

        true
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///  
    #[inline]
    pub(super) fn number_of_queued_contributors(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_contributor()).count()
    }

    ///
    /// Returns the total number of verifiers currently in the queue.
    ///
    #[inline]
    pub(super) fn number_of_queued_verifiers(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_verifier()).count()
    }

    ///
    /// Updates the current round height stored in the coordinator state.
    ///
    #[inline]
    pub(super) fn set_current_round_height(&mut self, current_round_height: u64) {
        self.status = CoordinatorStatus::Initialized;
        self.current_round_height = Some(current_round_height);
    }

    ///
    /// Adds the given chunk ID to the participant to process.
    ///
    #[inline]
    pub(super) fn push_front_chunk_id(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
    ) -> Result<(), CoordinatorError> {
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
    pub(super) fn pop_chunk_id(&mut self, participant: &Participant) -> Result<u64, CoordinatorError> {
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
    pub(super) fn acquired_lock(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
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
    pub(super) fn released_lock(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
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
    pub(super) fn add_pending_verification(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the set pending verification does not already contain the chunk ID.
        if self.pending_verification.contains_key(&chunk_id) {
            return Err(CoordinatorError::ChunkIdAlreadyAdded);
        }

        let verifier = match self
            .current_verifiers
            .par_iter()
            .min_by_key(|(_, v)| v.pending_chunks.len() + v.locked_chunks.len())
        {
            Some((verifier, _verifier_info)) => verifier.clone(),
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
    pub(super) fn remove_pending_verification(&mut self, chunk_id: u64) -> Result<(), CoordinatorError> {
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
    #[deprecated]
    #[allow(dead_code)]
    #[inline]
    pub(super) fn add_to_next_round(
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
    #[deprecated]
    #[allow(dead_code)]
    #[inline]
    pub(super) fn remove_from_next_round(&mut self, participant: Participant) -> Result<(), CoordinatorError> {
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
    pub(super) fn add_to_queue(
        &mut self,
        participant: Participant,
        reliability_score: u8,
    ) -> Result<(), CoordinatorError> {
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
    pub(super) fn remove_from_queue(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Check that the participant is exists in the queue.
        if !self.queue.contains_key(participant) {
            return Err(CoordinatorError::ParticipantMissing);
        }

        // Remove the participant from the queue.
        self.queue.remove(participant);

        Ok(())
    }

    ///
    /// Updates the state of the queue for all waiting participants.
    ///
    #[inline]
    pub(super) fn update_queue(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the next round height.
        let next_round = match self.current_round_height {
            Some(round_height) => round_height + 1,
            _ => return Err(CoordinatorError::RoundHeightNotSet),
        };

        // Sort the participants in the queue by reliability.
        let mut queue: Vec<_> = self.queue.clone().into_par_iter().map(|(p, (r, _))| (p, r)).collect();
        queue.par_sort_by(|a, b| (b.1).cmp(&a.1));

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
                let assigned_round = next_round + index as u64;
                trace!(
                    "Assigning contributor {} with reliability {} in queue to round {}",
                    contributor,
                    reliability,
                    assigned_round
                );
                updated_queue.insert(contributor.clone(), (*reliability, Some(assigned_round)));
            }
        }

        // Update assigned round height for each verifier.
        for (index, round) in verifiers.chunks(maximum_verifiers).enumerate() {
            for (verifier, reliability) in round.into_iter() {
                let assigned_round = next_round + index as u64;
                trace!(
                    "Assigning verifier {} with reliability {} in queue to round {}",
                    verifier,
                    reliability,
                    assigned_round
                );
                updated_queue.insert(verifier.clone(), (*reliability, Some(assigned_round)));
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
    pub(super) fn update_current_contributors(&mut self) -> Result<(), CoordinatorError> {
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
    pub(super) fn update_current_verifiers(&mut self) -> Result<(), CoordinatorError> {
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
    pub(super) fn update_dropped_participants(&mut self) -> Result<(), CoordinatorError> {
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
    pub(super) fn update_banned_participants(&mut self) -> Result<(), CoordinatorError> {
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
    pub(super) fn precommit_next_round(
        &mut self,
        next_round_height: u64,
    ) -> Result<(Vec<Participant>, Vec<Participant>), CoordinatorError> {
        trace!("Attempting to run precommit for round {}", next_round_height);

        // Check that the coordinator is not already in precommit.
        if self.status == CoordinatorStatus::Precommit {
            return Err(CoordinatorError::NextRoundAlreadyInPrecommit);
        }

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

        // Initialize the precommit stage for the next round.
        let mut queue = self.queue.clone();
        let mut next = HashMap::default();
        let mut next_contributors = Vec::with_capacity(number_of_contributors);
        let mut next_verifiers = Vec::with_capacity(number_of_verifiers);

        // Create the initial chunk locking sequence for each contributor.
        {
            // Sort the contributors by their reliability (in order of highest to lowest number).
            contributors.par_sort_by(|a, b| ((b.1).0).cmp(&(&a.1).0));

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
                next.insert(participant.clone(), participant_info);

                // Remove the contributor from the queue.
                queue.remove(&participant);

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
            next.insert(participant.clone(), participant_info);

            // Remove the verifier from the queue.
            queue.remove(&participant);

            // Add the next round contributors to the return output.
            next_verifiers.push(participant);
        }

        // Update the coordinator state to the updated queue and next map.
        self.queue = queue;
        self.next = next;

        // Set the coordinator status to precommit.
        self.status = CoordinatorStatus::Precommit;

        Ok((next_contributors, next_verifiers))
    }

    ///
    /// Executes transition of the coordinator state from the current round to the next round.
    ///
    /// This function always executes without failure or exists without modifying state
    /// if the commit was unauthorized.
    ///
    #[inline]
    pub(super) fn commit_next_round(&mut self) {
        // Check that the coordinator is authorized to advance to the next round.
        if self.status != CoordinatorStatus::Precommit {
            error!("Coordinator is not in the precommit stage and cannot advance the round");
            return;
        }

        // Increment the current round height.
        self.current_round_height = match self.current_round_height {
            Some(current_round_height) => {
                trace!("Coordinator has advanced to round {}", current_round_height + 1);
                Some(current_round_height + 1)
            }
            None => {
                error!("Coordinator cannot commit to the next round without initializing the round height");
                return;
            }
        };

        // Set the current status to the commit.
        self.status = CoordinatorStatus::Commit;

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
    /// This function always executes without failure or exists without modifying state
    /// if the rollback was unauthorized.
    ///
    #[inline]
    pub(super) fn rollback_next_round(&mut self) {
        // Check that the coordinator is authorized to rollback.
        if self.status != CoordinatorStatus::Precommit {
            error!("Coordinator is not in the precommit stage and cannot rollback");
            return;
        }

        // Set the current status to the commit.
        self.status = CoordinatorStatus::Rollback;

        // Add each participant back into the queue.
        for (participant, participant_info) in &self.next {
            self.queue.insert(
                participant.clone(),
                (participant_info.reliability, Some(participant_info.round_height)),
            );
        }

        // Reset the next round map.
        self.next = HashMap::new();

        trace!("Coordinator has rolled back");
    }

    ///
    /// Returns the status of the coordinator state.
    ///
    #[inline]
    pub(super) fn status_report(&self) -> String {
        let number_of_current_contributors = self.current_contributors.len();
        let number_of_current_verifiers = self.current_verifiers.len();
        let number_of_pending_verifications = self.pending_verification.len();

        // Parse the queue for assigned contributors and verifiers of the next round.
        let next_round_height = self.current_round_height.unwrap_or_default() + 1;
        let number_of_assigned_contributors = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();
        let number_of_assigned_verifiers = self
            .queue
            .clone()
            .into_par_iter()
            .filter(|(p, (_, rh))| p.is_verifier() && rh.unwrap_or_default() == next_round_height)
            .count();

        let number_of_queued_contributors = self.number_of_queued_contributors();
        let number_of_queued_verifiers = self.number_of_queued_verifiers();

        let number_of_dropped_participants = self.dropped.len();
        let number_of_banned_participants = self.banned.len();

        format!(
            r#"
    {} contributors and {} verifiers in the current round
    {} chunks are pending verification

    {} contributors and {} verifiers assigned to the next round
    {} contributors and {} verifiers in queue for the ceremony

    {} participants dropped
    {} participants banned
        "#,
            number_of_current_contributors,
            number_of_current_verifiers,
            number_of_pending_verifications,
            number_of_assigned_contributors,
            number_of_assigned_verifiers,
            number_of_queued_contributors,
            number_of_queued_verifiers,
            number_of_dropped_participants,
            number_of_banned_participants
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{objects::participant::*, testing::prelude::*, CoordinatorState};

    #[test]
    fn test_new() {
        // Initialize a new coordinator state.
        let state = CoordinatorState::new(TEST_ENVIRONMENT.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }

    #[test]
    fn test_set_current_round_height() {
        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(TEST_ENVIRONMENT.clone());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.set_current_round_height(current_round_height);
        assert_eq!(Some(current_round_height), state.current_round_height);
    }

    #[test]
    fn test_add_to_queue_contributor() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor of the coordinator.
        let contributor = environment.coordinator_contributor();
        assert!(contributor.is_contributor());

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the contributor of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Attempt to add the contributor again.
        for _ in 0..10 {
            let result = state.add_to_queue(contributor.clone(), 10);
            assert!(result.is_err());
            assert_eq!(1, state.queue.len());
        }
    }

    #[test]
    fn test_add_to_queue_verifier() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the verifier of the coordinator.
        let verifier = environment.coordinator_verifier();
        assert!(verifier.is_verifier());

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the verifier of the coordinator.
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Attempt to add the verifier again.
        for _ in 0..10 {
            let result = state.add_to_queue(verifier.clone(), 10);
            assert!(result.is_err());
            assert_eq!(1, state.queue.len());
        }
    }

    #[test]
    fn test_update_queue() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = environment.coordinator_contributor();
        let verifier = environment.coordinator_verifier();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.set_current_round_height(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, Some(6))), participant);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, Some(6))), participant);

        // Attempt to add the contributor and verifier again.
        for _ in 0..10 {
            let contributor_result = state.add_to_queue(contributor.clone(), 10);
            let verifier_result = state.add_to_queue(verifier.clone(), 10);
            assert!(contributor_result.is_err());
            assert!(verifier_result.is_err());
            assert_eq!(2, state.queue.len());
        }
    }

    #[test]
    fn test_update_queue_assignment() {
        test_logger();

        let environment = TEST_ENVIRONMENT.clone();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.set_current_round_height(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add (2 * maximum_contributors_per_round) to the queue.
        let maximum_contributors_per_round = environment.maximum_contributors_per_round();
        let number_of_contributors_in_queue = 2 * maximum_contributors_per_round;
        for id in 1..=number_of_contributors_in_queue {
            trace!("Adding contributor with ID {}", id);

            // Add a unique contributor.
            let contributor = Participant::Contributor(id.to_string());
            let reliability = 10 - id as u8;
            state.add_to_queue(contributor.clone(), reliability).unwrap();
            assert_eq!(id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the contributor from the queue.
            let participant = state.queue.get(&contributor);
            assert_eq!(Some(&(reliability, None)), participant);

            // Update the state of the queue.
            state.update_queue().unwrap();
            assert_eq!(id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the contributor from the queue.
            let participant = state.queue.get(&contributor);
            match id <= maximum_contributors_per_round {
                true => assert_eq!(Some(&(reliability, Some(6))), participant),
                false => assert_eq!(Some(&(reliability, Some(7))), participant),
            }
        }

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(number_of_contributors_in_queue, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add (2 * maximum_verifiers_per_round) to the queue.
        let maximum_verifiers_per_round = environment.maximum_verifiers_per_round();
        let number_of_verifiers_in_queue = 2 * maximum_verifiers_per_round;
        for id in 1..=number_of_verifiers_in_queue {
            trace!("Adding verifier with ID {}", id);

            // Add a unique verifier.
            let verifier = Participant::Verifier(id.to_string());
            let reliability = 10 - id as u8;
            state.add_to_queue(verifier.clone(), reliability).unwrap();
            assert_eq!(number_of_contributors_in_queue + id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the verifier from the queue.
            let participant = state.queue.get(&verifier);
            assert_eq!(Some(&(reliability, None)), participant);

            // Update the state of the queue.
            state.update_queue().unwrap();
            assert_eq!(number_of_contributors_in_queue + id, state.queue.len());
            assert_eq!(0, state.next.len());
            assert_eq!(Some(current_round_height), state.current_round_height);

            // Fetch the verifier from the queue.
            let participant = state.queue.get(&verifier);
            match id <= maximum_verifiers_per_round {
                true => assert_eq!(Some(&(reliability, Some(6))), participant),
                false => assert_eq!(Some(&(reliability, Some(7))), participant),
            }
        }

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(
            number_of_contributors_in_queue + number_of_verifiers_in_queue,
            state.queue.len()
        );
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
    }

    #[test]
    fn test_remove_from_queue_contributor() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor of the coordinator.
        let contributor = environment.coordinator_contributor();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the contributor of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Fetch the contributor from the queue.
        let participant = state.queue.get(&contributor);
        assert_eq!(Some(&(10, None)), participant);

        // Remove the contributor from the queue.
        state.remove_from_queue(&contributor).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Attempt to remove the contributor again.
        for _ in 0..10 {
            let result = state.remove_from_queue(&contributor);
            assert!(result.is_err());
            assert_eq!(0, state.queue.len());
        }
    }

    #[test]
    fn test_remove_to_queue_verifier() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the verifier of the coordinator.
        let verifier = environment.coordinator_verifier();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());

        // Add the verifier of the coordinator.
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(1, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Fetch the verifier from the queue.
        let participant = state.queue.get(&verifier);
        assert_eq!(Some(&(10, None)), participant);

        // Remove the verifier from the queue.
        state.remove_from_queue(&verifier).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(None, state.current_round_height);

        // Attempt to remove the verifier again.
        for _ in 0..10 {
            let result = state.remove_from_queue(&verifier);
            assert!(result.is_err());
            assert_eq!(0, state.queue.len());
        }
    }

    #[test]
    fn test_commit_next_round() {
        test_logger();

        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = environment.coordinator_contributor();
        let verifier = environment.coordinator_verifier();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.set_current_round_height(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&contributor));
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&verifier));

        // TODO (howardwu): Add individual tests and assertions after each of these operations.
        {
            // Update the state of current round contributors.
            state.update_current_contributors().unwrap();

            // Update the state of current round verifiers.
            state.update_current_verifiers().unwrap();

            // Drop disconnected participants from the current round.
            state.update_dropped_participants().unwrap();

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants().unwrap();
        }

        // Determine if current round is finished and precommit to next round is ready.
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let next_round_height = current_round_height + 1;
        let _precommit = state.precommit_next_round(next_round_height).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(2, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(!state.is_precommit_next_round_ready());

        // Advance the coordinator to the next round.
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
        assert!(!state.is_current_round_finished());
        assert!(!state.is_precommit_next_round_ready());
    }

    #[test]
    fn test_rollback_next_round() {
        test_logger();

        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = environment.coordinator_contributor();
        let verifier = environment.coordinator_verifier();

        // Initialize a new coordinator state.
        let mut state = CoordinatorState::new(environment.clone());
        assert_eq!(0, state.queue.len());
        assert_eq!(None, state.current_round_height);

        // Set the current round height for coordinator state.
        let current_round_height = 5;
        state.set_current_round_height(current_round_height);
        assert_eq!(0, state.queue.len());
        assert_eq!(Some(current_round_height), state.current_round_height);

        // Add the contributor and verifier of the coordinator.
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        assert_eq!(2, state.queue.len());

        // Update the state of the queue.
        state.update_queue().unwrap();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&contributor));
        assert_eq!(Some(&(10, Some(6))), state.queue.get(&verifier));

        // TODO (howardwu): Add individual tests and assertions after each of these operations.
        {
            // Update the state of current round contributors.
            state.update_current_contributors().unwrap();

            // Update the state of current round verifiers.
            state.update_current_verifiers().unwrap();

            // Drop disconnected participants from the current round.
            state.update_dropped_participants().unwrap();

            // Ban any participants who meet the coordinator criteria.
            state.update_banned_participants().unwrap();
        }

        // Determine if current round is finished and precommit to next round is ready.
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let _precommit = state.precommit_next_round(current_round_height + 1).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(2, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert!(state.is_current_round_finished());
        assert!(!state.is_precommit_next_round_ready());

        // Rollback the coordinator to the current round.
        state.rollback_next_round();
        assert_eq!(2, state.queue.len());
        assert_eq!(0, state.next.len());
        assert_eq!(Some(current_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());
    }
}
