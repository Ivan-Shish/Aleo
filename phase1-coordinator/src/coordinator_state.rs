use crate::{environment::Environment, objects::participant::*, CoordinatorError};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{
    de::{self, Deserializer, Error},
    ser::Serializer,
    Deserialize,
    Serialize,
};
use std::{
    collections::{HashMap, HashSet, LinkedList},
    str::FromStr,
};
use tracing::*;

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
    /// The set of chunk IDs that this participant is computing.
    locked_chunks: HashSet<u64>,
    /// The list of (chunk ID, contribution ID) tasks that this participant has left to compute.
    pending_tasks: LinkedList<Task>,
    /// The list of (chunk ID, contribution ID) tasks that this participant has computed.
    completed_tasks: LinkedList<Task>,
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
            locked_chunks: HashSet::new(),
            pending_tasks: LinkedList::new(),
            completed_tasks: LinkedList::new(),
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

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return false;
        }

        // Check that the participant has no more pending tasks.
        if !self.pending_tasks.is_empty() {
            return false;
        }

        // Check that if the participant is a contributor, that they completed tasks.
        if self.id.is_contributor() && self.completed_tasks.is_empty() {
            return false;
        }

        // Check if the participant has already finished the round.
        self.finished_at.is_some()
    }

    ///
    /// Assigns the participant to the given chunks for the current round,
    /// and sets the start time as the current time.
    ///
    #[inline]
    fn start(&mut self, tasks: LinkedList<Task>) -> Result<(), CoordinatorError> {
        // Check that the participant has a valid round height set.
        if self.round_height == 0 {
            return Err(CoordinatorError::ParticipantRoundHeightInvalid);
        }

        // Check that the participant has not already started in the round.
        if self.started_at.is_some() || self.dropped_at.is_some() || self.finished_at.is_some() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Check that the participant has no locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyHasLockedChunks);
        }

        // Check that the participant has no pending tasks.
        if !self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Check that the participant has not completed any tasks yet.
        if !self.completed_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantAlreadyStarted);
        }

        // Fetch the current time.
        let now = Utc::now();

        // Set the participant info to reflect them starting now.
        self.last_seen = now;
        self.started_at = Some(now);
        self.pending_tasks = tasks;

        Ok(())
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task in FIFO order for the participant to process.
    ///
    #[inline]
    fn push_back_task(&mut self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Set the task as the given chunk ID and contribution ID.
        let task = Task::new(chunk_id, contribution_id);

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

        // Check that if the participant is a contributor, this chunk is not currently locked.
        if self.id.is_contributor() && self.locked_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Check that the task was not already added to the pending tasks.
        if self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Add the task to the back of the pending tasks.
        self.pending_tasks.push_back(task);

        Ok(())
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task in LIFO order for the participant to process.
    ///
    #[inline]
    fn push_front_task(&mut self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Set the task as the given chunk ID and contribution ID.
        let task = Task::new(chunk_id, contribution_id);

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

        // Check that if the participant is a contributor, this chunk is not currently locked.
        if self.id.is_contributor() && self.locked_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Check that the task was not already added to the pending tasks.
        if self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyAddedChunk);
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyWorkingOnChunk);
        }

        // Add the task to the front of the pending tasks.
        self.pending_tasks.push_front(task);

        Ok(())
    }

    ///
    /// Pops the next (chunk ID, contribution ID) task the participant should process,
    /// in FIFO order when added to the linked list.
    ///
    #[inline]
    fn pop_task(&mut self) -> Result<Task, CoordinatorError> {
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

        // Check that the participant has pending tasks.
        if self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasNoRemainingTasks);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Fetch the next task in order as stored.
        match self.pending_tasks.pop_front() {
            Some(task) => Ok(task),
            None => Err(CoordinatorError::ParticipantHasNoRemainingTasks),
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

        // Check that this chunk is not currently locked by the participant.
        if self.locked_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantAlreadyHasLockedChunk);
        }

        // Check that if the participant is a contributor, this chunk was not already pending.
        if self.id.is_contributor()
            && self
                .pending_tasks
                .par_iter()
                .filter(|task| task.contains(chunk_id))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantUnauthorizedForChunkId);
        }

        // Check that if the participant is a contributor, this chunk was not already completed.
        if self.id.is_contributor()
            && self
                .completed_tasks
                .par_iter()
                .filter(|task| task.contains(chunk_id))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Adds the given chunk ID to the locked chunks.
        self.locked_chunks.insert(chunk_id);

        Ok(())
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task to the list of completed tasks
    /// and removes the given chunk ID from the locked chunks held by this participant.
    ///
    #[inline]
    fn completed_task(&mut self, chunk_id: u64, contribution_id: u64) -> Result<(), CoordinatorError> {
        // Set the task as the given chunk ID and contribution ID.
        let task = Task::new(chunk_id, contribution_id);

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
        if !self.locked_chunks.contains(&chunk_id) {
            return Err(CoordinatorError::ParticipantDidntLockChunkId);
        }

        // Check that the participant does not have a pending task remaining for this.
        if self.pending_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantStillHasTaskAsPending);
        }

        // Check that the participant has not already completed the task.
        if self.completed_tasks.contains(&task) {
            return Err(CoordinatorError::ParticipantAlreadyFinishedTask);
        }

        // Check that if the participant is a contributor, this chunk was not already completed.
        if self.id.is_contributor()
            && self
                .completed_tasks
                .par_iter()
                .filter(|task| task.contains(chunk_id))
                .count()
                > 0
        {
            return Err(CoordinatorError::ParticipantAlreadyFinishedChunk);
        }

        // Update the last seen time.
        self.last_seen = Utc::now();

        // Remove the given chunk ID from the locked chunks.
        self.locked_chunks.remove(&chunk_id);

        // Add the task to the completed tasks.
        self.completed_tasks.push_back(task);

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

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Check that the participant has no more pending tasks.
        if !self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
        }

        // Check that if the participant is a contributor, that they completed tasks.
        if self.id.is_contributor() && self.completed_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantDidNotDoWork);
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

        // Check that the participant has no more locked chunks.
        if !self.locked_chunks.is_empty() {
            return Err(CoordinatorError::ParticipantStillHasLocks);
        }

        // Check that the participant has no more pending tasks.
        if !self.pending_tasks.is_empty() {
            return Err(CoordinatorError::ParticipantHasRemainingTasks);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorState {
    /// The parameters and settings of this coordinator.
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
    /// The map of tasks pending verification in the current round.
    pending_verification: HashMap<Task, Participant>,
    /// The map of each round height to the corresponding contributors from that round.
    finished_contributors: HashMap<u64, HashMap<Participant, ParticipantInfo>>,
    /// The map of each round height to the corresponding verifiers from that round.
    finished_verifiers: HashMap<u64, HashMap<Participant, ParticipantInfo>>,
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
    /// Returns `true` if the given participant is a contributor in the queue.
    ///
    #[inline]
    pub(super) fn is_queue_contributor(&self, participant: &Participant) -> bool {
        participant.is_contributor() && self.queue.contains_key(participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier in the queue.
    ///
    #[inline]
    pub(super) fn is_queue_verifier(&self, participant: &Participant) -> bool {
        participant.is_verifier() && self.queue.contains_key(participant)
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///  
    #[inline]
    pub(super) fn number_of_queue_contributors(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_contributor()).count()
    }

    ///
    /// Returns the total number of verifiers currently in the queue.
    ///
    #[inline]
    pub(super) fn number_of_queue_verifiers(&self) -> usize {
        self.queue.par_iter().filter(|(p, _)| p.is_verifier()).count()
    }

    ///
    /// Returns a list of the contributors currently in the queue.
    ///
    #[inline]
    pub(super) fn queue_contributors(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        self.queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_contributor())
            .collect()
    }

    ///
    /// Returns a list of the verifiers currently in the queue.
    ///
    #[inline]
    pub(super) fn queue_verifiers(&self) -> Vec<(Participant, (u8, Option<u64>))> {
        self.queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_verifier())
            .collect()
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
    /// Returns the current round height stored in the coordinator state.
    ///
    /// This function returns `0` if the current round height has not been set.
    ///
    #[inline]
    pub(super) fn current_round_height(&mut self) -> u64 {
        self.current_round_height.unwrap_or_default()
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
    /// Returns `true` if all participants in the current round have no more pending chunks.
    ///
    #[inline]
    pub(super) fn is_current_round_finished(&self) -> bool {
        // Check that all contributions have undergone verification.
        self.pending_verification.is_empty()
            // Check that all current contributors are finished.
            && self.current_contributors.is_empty()
            // Check that all current verifiers are finished.
            && self.current_verifiers.is_empty()
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
    /// Adds the given (chunk ID, contribution ID) pair to the participant to process.
    ///
    #[inline]
    pub(super) fn push_front_task(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<(), CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Add the chunk ID to the pending chunks of the given participant.
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                Some(participant) => Ok(participant.push_front_task(chunk_id, contribution_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                Some(participant) => Ok(participant.push_front_task(chunk_id, contribution_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Pops the next (chunk ID, contribution ID) task that the participant should process.
    ///
    #[inline]
    pub(super) fn pop_task(&mut self, participant: &Participant) -> Result<Task, CoordinatorError> {
        // Fetch the contributor and verifier chunk lock limit.
        let contributor_limit = self.environment.contributor_lock_chunk_limit();
        let verifier_limit = self.environment.verifier_lock_chunk_limit();

        // Remove the next chunk ID from the pending chunks of the given participant.
        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Check that the participant is holding less than the chunk lock limit.
                Some(participant_info) => match participant_info.locked_chunks.len() < contributor_limit {
                    true => Ok(participant_info.pop_task()?),
                    false => Err(CoordinatorError::ParticipantHasLockedMaximumChunks),
                },
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Check that the participant is holding less than the chunk lock limit.
                Some(participant_info) => match participant_info.locked_chunks.len() < verifier_limit {
                    true => Ok(participant_info.pop_task()?),
                    false => Err(CoordinatorError::ParticipantHasLockedMaximumChunks),
                },
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Adds the given chunk ID to the locks held by the given participant.
    ///
    #[inline]
    pub(super) fn acquired_lock(&mut self, participant: &Participant, chunk_id: u64) -> Result<(), CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Acquire the chunk lock for the contributor.
                Some(participant) => Ok(participant.acquired_lock(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Acquire the chunk lock for the verifier.
                Some(participant) => Ok(participant.acquired_lock(chunk_id)?),
                None => Err(CoordinatorError::ParticipantNotFound),
            },
        }
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task to the pending verification set.
    /// The verification task is then assigned to the verifier with the least number of tasks in its queue.
    ///
    /// On success, this function returns the verifier that was assigned to the verification task.
    ///
    #[inline]
    pub(super) fn add_pending_verification(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the pending verification set does not already contain the chunk ID.
        if self
            .pending_verification
            .contains_key(&Task::new(chunk_id, contribution_id))
        {
            return Err(CoordinatorError::ChunkIdAlreadyAdded);
        }

        let verifier = match self
            .current_verifiers
            .par_iter()
            .min_by_key(|(_, v)| v.pending_tasks.len() + v.locked_chunks.len())
        {
            Some((verifier, _verifier_info)) => verifier.clone(),
            None => return Err(CoordinatorError::VerifierMissing),
        };

        info!(
            "Assigning (chunk {}, contribution {}) to {} for verification",
            chunk_id, contribution_id, verifier
        );

        match self.current_verifiers.get_mut(&verifier) {
            Some(verifier_info) => verifier_info.push_back_task(chunk_id, contribution_id)?,
            None => return Err(CoordinatorError::VerifierMissing),
        };

        self.pending_verification
            .insert(Task::new(chunk_id, contribution_id), verifier.clone());

        Ok(verifier)
    }

    ///
    /// Remove the given (chunk ID, contribution ID) task from the map of chunks that are pending verification.
    ///
    /// On success, this function returns the verifier that completed the verification task.
    ///
    #[inline]
    pub(super) fn remove_pending_verification(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the set pending verification does not already contain the chunk ID.
        if !self
            .pending_verification
            .contains_key(&Task::new(chunk_id, contribution_id))
        {
            return Err(CoordinatorError::ChunkIdMissing);
        }

        debug!(
            "Removing (chunk {}, contribution {}) from the pending verifications",
            chunk_id, contribution_id
        );

        // Remove the task from the pending verification.
        let verifier = self
            .pending_verification
            .remove(&Task::new(chunk_id, contribution_id))
            .ok_or(CoordinatorError::VerifierMissing)?;

        Ok(verifier)
    }

    ///
    /// Adds the given (chunk ID, contribution ID) task to the completed tasks of the given participant,
    /// and removes the chunk ID from the locks held by the given participant.
    ///
    /// On success, this function returns the verifier assigned to the verification task.
    ///
    #[inline]
    pub(super) fn completed_task(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<Participant, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        match participant {
            Participant::Contributor(_) => match self.current_contributors.get_mut(participant) {
                // Adds the task to the list of completed tasks for the contributor,
                // and add the task to the pending verification set.
                Some(participant) => {
                    participant.completed_task(chunk_id, contribution_id)?;
                    Ok(self.add_pending_verification(chunk_id, contribution_id)?)
                }
                None => Err(CoordinatorError::ParticipantNotFound),
            },
            Participant::Verifier(_) => match self.current_verifiers.get_mut(participant) {
                // Adds the task to the list of completed tasks for the verifier,
                // and remove the task from the pending verification set.
                Some(participant) => {
                    participant.completed_task(chunk_id, contribution_id)?;
                    Ok(self.remove_pending_verification(chunk_id, contribution_id)?)
                }
                None => Err(CoordinatorError::ParticipantNotFound),
            },
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

        // Parse the queue participants into contributors and verifiers,
        // and check that they are not banned participants.
        let contributors: Vec<(_, _)> = queue
            .clone()
            .into_par_iter()
            .filter(|(p, _)| p.is_contributor() && !self.banned.contains(&p))
            .collect();
        let verifiers: Vec<(_, _)> = queue
            .into_par_iter()
            .filter(|(p, _)| p.is_verifier() && !self.banned.contains(&p))
            .collect();

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
        // Fetch the current round height.
        let current_round_height = self.current_round_height.ok_or(CoordinatorError::RoundHeightNotSet)?;

        // Fetch the current number of contributors.
        let number_of_current_contributors = self.current_contributors.len();

        // Initialize a map for newly finished contributors.
        let mut newly_finished: HashMap<Participant, ParticipantInfo> = HashMap::new();

        // Iterate through all of the current contributors and check if they have finished.
        self.current_contributors = self
            .current_contributors
            .clone()
            .into_iter()
            .filter(|(contributor, contributor_info)| {
                // Check if the contributor has finished.
                if contributor_info.is_finished() {
                    return false;
                }

                // Attempt to set the contributor as finished.
                let mut finished_info = contributor_info.clone();
                if let Err(_) = finished_info.finish() {
                    return true;
                }

                // Add the contributor to the set of finished contributors.
                newly_finished.insert(contributor.clone(), finished_info.clone());

                debug!("{} has finished", contributor);
                false
            })
            .collect();

        // Check that the update preserves the same number of contributors.
        if number_of_current_contributors != self.current_contributors.len() + newly_finished.len() {
            return Err(CoordinatorError::RoundUpdateCorruptedStateOfContributors);
        }

        trace!("Marking {} current contributors as finished", newly_finished.len());

        // Update the map of finished contributors.
        match self.finished_contributors.get_mut(&current_round_height) {
            Some(contributors) => contributors.extend(newly_finished.into_iter()),
            None => return Err(CoordinatorError::RoundCommitFailedOrCorrupted),
        };

        Ok(())
    }

    ///
    /// Updates the state of verifiers in the current round.
    ///
    /// This function should never be run prior to calling `update_current_contributors`.
    ///
    #[inline]
    pub(super) fn update_current_verifiers(&mut self) -> Result<(), CoordinatorError> {
        // Check if the contributors are finished.
        let is_contributors_finished = self.current_contributors.is_empty();

        // If all contributors are finished, this means there are no new verification jobs
        // to be added to the pending verifications queue. So if a verifier is finished
        // with their verifications, then they are finished for this round.
        if is_contributors_finished {
            // Fetch the current round height.
            let current_round_height = self.current_round_height.ok_or(CoordinatorError::RoundHeightNotSet)?;

            // Fetch the current number of verifiers.
            let number_of_current_verifiers = self.current_verifiers.len();

            // Initialize a map for newly finished verifiers.
            let mut newly_finished: HashMap<Participant, ParticipantInfo> = HashMap::new();

            // Iterate through all of the current verifiers and check if they have finished.
            self.current_verifiers = self
                .current_verifiers
                .clone()
                .into_iter()
                .filter(|(verifier, verifier_info)| {
                    // Check if the verifier has finished.
                    if verifier_info.is_finished() {
                        return false;
                    }

                    // Attempt to set the verifier as finished.
                    let mut finished_info = verifier_info.clone();
                    if let Err(_) = finished_info.finish() {
                        return true;
                    }

                    // Add the verifier to the set of finished verifier.
                    newly_finished.insert(verifier.clone(), finished_info.clone());

                    debug!("{} has finished", verifier);
                    false
                })
                .collect();

            // Check that the update preserves the same number of verifiers.
            if number_of_current_verifiers != self.current_verifiers.len() + newly_finished.len() {
                return Err(CoordinatorError::RoundUpdateCorruptedStateOfVerifiers);
            }

            trace!("Marking {} current verifiers as finished", newly_finished.len());

            // Update the map of finished verifiers.
            match self.finished_verifiers.get_mut(&current_round_height) {
                Some(contributors) => contributors.extend(newly_finished.into_iter()),
                None => return Err(CoordinatorError::RoundCommitFailedOrCorrupted),
            };
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

        // Check that the coordinator is not already in the precommit stage.
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
            /* ***********************************************************************************
             *   The following is the approach for contributor task assignments.
             * ***********************************************************************************
             *
             *   N := NUMBER_OF_CONTRIBUTORS
             *   BUCKET_SIZE := NUMBER_OF_CHUNKS / NUMBER_OF_CONTRIBUTORS
             *
             * ***********************************************************************************
             *
             *   [    BUCKET 1    |    BUCKET 2    |    BUCKET 3    |  . . .  |    BUCKET N    ]
             *
             *   [  CONTRIBUTOR 1  --------------------------------------------------------->  ]
             *   [  ------------->  CONTRIBUTOR 2  ------------------------------------------  ]
             *   [  ------------------------------>  CONTRIBUTOR 3  -------------------------  ]
             *   [                                        .                                    ]
             *   [                                        .                                    ]
             *   [                                        .                                    ]
             *   [  --------------------------------------------------------->  CONTRIBUTOR N  ]
             *
             * ***********************************************************************************
             *
             *   1. Sort the round contributors from most reliable to least reliable.
             *
             *   2. Assign CONTRIBUTOR 1 to BUCKET 1, CONTRIBUTOR 2 to BUCKET 2,
             *      CONTRIBUTOR 3 to BUCKET 3, ..., CONTRIBUTOR N to BUCKET N,
             *      as the starting INDEX to contribute to in the round.
             *
             *   3. Construct the set of tasks for each contributor as follows:
             *
             *      for ID in 0..NUMBER_OF_CHUNKS:
             *          CHUNK_ID := (INDEX * BUCKET_SIZE + ID) % NUMBER_OF_CHUNKS
             *          CONTRIBUTION_ID := INDEX.
             *
             * ***********************************************************************************
             */

            // Sort the contributors by their reliability (in order of highest to lowest number).
            contributors.par_sort_by(|a, b| ((b.1).0).cmp(&(&a.1).0));

            // Fetch the number of chunks and bucket size.
            let number_of_chunks = self.environment.number_of_chunks() as u64;
            let bucket_size = number_of_chunks / number_of_contributors as u64;

            // Set the chunk ID ordering for each contributor.
            for (bucket_index, (participant, (reliability, next_round))) in contributors.into_iter().enumerate() {
                // Determine the starting and ending indices.
                let start = bucket_index as u64 * bucket_size;
                let end = start + number_of_chunks;

                // Add the tasks in FIFO ordering.
                let mut tasks = LinkedList::new();
                for index in start..end {
                    let chunk_id = index % number_of_chunks;
                    let contribution_id = bucket_index + 1;
                    tasks.push_back(Task::new(chunk_id, contribution_id as u64));
                }

                // Check that each participant is storing the correct round height.
                if next_round != next_round_height && next_round != current_round_height + 1 {
                    warn!("Contributor claims round is {}, not {}", next_round, next_round_height);
                    return Err(CoordinatorError::RoundHeightMismatch);
                }

                // Initialize the participant info for the contributor.
                let mut participant_info = ParticipantInfo::new(participant.clone(), next_round_height, reliability);
                participant_info.start(tasks)?;

                // Check that the chunk IDs are set in the participant information.
                if participant_info.pending_tasks.is_empty() {
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
        let next_round_height = match self.current_round_height {
            Some(current_round_height) => {
                trace!("Coordinator has advanced to round {}", current_round_height + 1);
                current_round_height + 1
            }
            None => {
                error!("Coordinator cannot commit to the next round without initializing the round height");
                return;
            }
        };
        self.current_round_height = Some(next_round_height);

        // Set the current status to the commit.
        self.status = CoordinatorStatus::Commit;

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

        // Initialize the finished contributors map for the next round.
        self.finished_contributors.insert(next_round_height, HashMap::new());

        // Initialize the finished verifiers map for the next round.
        self.finished_verifiers.insert(next_round_height, HashMap::new());

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
        let current_round_height = self.current_round_height.unwrap_or_default();
        let next_round_height = current_round_height + 1;

        let current_round_finished = match self.is_current_round_finished() {
            true => format!("Round {} is finished", current_round_height),
            false => format!("Round {} is in progress", current_round_height),
        };
        let precommit_next_round_ready = match self.is_precommit_next_round_ready() {
            true => format!("Round {} is ready to begin", next_round_height),
            false => format!("Round {} is awaiting participants", next_round_height),
        };

        let number_of_current_contributors = self.current_contributors.len();
        let number_of_current_verifiers = self.current_verifiers.len();
        let number_of_finished_contributors = self
            .finished_contributors
            .get(&current_round_height)
            .get_or_insert(&HashMap::new())
            .len();
        let number_of_finished_verifiers = self
            .finished_verifiers
            .get(&current_round_height)
            .get_or_insert(&HashMap::new())
            .len();
        let number_of_pending_verifications = self.pending_verification.len();

        // Parse the queue for assigned contributors and verifiers of the next round.
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

        let number_of_queue_contributors = self.number_of_queue_contributors();
        let number_of_queue_verifiers = self.number_of_queue_verifiers();

        let number_of_dropped_participants = self.dropped.len();
        let number_of_banned_participants = self.banned.len();

        format!(
            r#"
    ----------------------------------------------------------------
    ||                        STATUS REPORT                       ||
    ----------------------------------------------------------------

    | {}
    | {}

    | {} contributors and {} verifiers active in the current round
    | {} contributors and {} verifiers completed the current round
    | {} chunks are pending verification

    | {} contributors and {} verifiers assigned to the next round
    | {} contributors and {} verifiers in queue for the ceremony

    | {} participants dropped
    | {} participants banned

    "#,
            current_round_finished,
            precommit_next_round_ready,
            number_of_current_contributors,
            number_of_current_verifiers,
            number_of_finished_contributors,
            number_of_finished_verifiers,
            number_of_pending_verifications,
            number_of_assigned_contributors,
            number_of_assigned_verifiers,
            number_of_queue_contributors,
            number_of_queue_verifiers,
            number_of_dropped_participants,
            number_of_banned_participants
        )
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub(crate) struct Task {
    pub(crate) chunk_id: u64,
    pub(crate) contribution_id: u64,
}

impl Task {
    #[inline]
    pub fn new(chunk_id: u64, contribution_id: u64) -> Self {
        Self {
            chunk_id,
            contribution_id,
        }
    }

    #[inline]
    pub fn contains(&self, chunk_id: u64) -> bool {
        self.chunk_id == chunk_id
    }
}

impl Serialize for Task {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}/{}", self.chunk_id, self.contribution_id))
    }
}

impl<'de> Deserialize<'de> for Task {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Task, D::Error> {
        let s = String::deserialize(deserializer)?;

        let mut task = s.split("/");
        let chunk_id = task.next().ok_or(D::Error::custom("invalid chunk ID"))?;
        let contribution_id = task.next().ok_or(D::Error::custom("invalid contribution ID"))?;
        Ok(Task::new(
            u64::from_str(&chunk_id).map_err(de::Error::custom)?,
            u64::from_str(&contribution_id).map_err(de::Error::custom)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{coordinator_state::Task, objects::participant::*, testing::prelude::*, CoordinatorState};

    #[test]
    fn test_task() {
        let task = Task::new(0, 1);
        assert_eq!("\"0/1\"", serde_json::to_string(&task).unwrap());
        assert_eq!(task, serde_json::from_str("\"0/1\"").unwrap());
    }

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
        let contributor = test_coordinator_contributor(&environment).unwrap();
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
        let verifier = test_coordinator_verifier(&environment).unwrap();
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
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

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
        let contributor = test_coordinator_contributor(&environment).unwrap();

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
        let verifier = test_coordinator_verifier(&environment).unwrap();

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
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

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
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

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

    #[test]
    fn test_pop_and_complete_tasks_contributor() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.set_current_round_height(current_round_height);
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        state.precommit_next_round(next_round_height).unwrap();
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

        // Fetch the maximum number of tasks permitted for a contributor.
        let contributor_lock_chunk_limit = environment.contributor_lock_chunk_limit();
        for chunk_id in 0..contributor_lock_chunk_limit {
            // Fetch a pending task for the contributor.
            let task = state.pop_task(&contributor).unwrap();
            assert_eq!((chunk_id as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor, task.chunk_id).unwrap();
            state
                .completed_task(&contributor, chunk_id as u64, task.contribution_id)
                .unwrap();
            assert_eq!(chunk_id, state.pending_verification.len());
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(contributor_lock_chunk_limit, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());

        // Attempt to fetch past the permitted lock chunk limit.
        for _ in 0..10 {
            let try_task = state.pop_task(&contributor);
            assert!(try_task.is_err());
        }
    }

    #[test]
    fn test_pop_and_complete_tasks_verifier() {
        let environment = TEST_ENVIRONMENT.clone();

        // Fetch the contributor and verifier of the coordinator.
        let contributor = test_coordinator_contributor(&environment).unwrap();
        let verifier = test_coordinator_verifier(&environment).unwrap();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.set_current_round_height(current_round_height);
        state.add_to_queue(contributor.clone(), 10).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        state.precommit_next_round(next_round_height).unwrap();
        state.commit_next_round();
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());

        // Ensure that the verifier cannot pop a task prior to a contributor completing a task.
        let try_task = state.pop_task(&verifier);
        assert!(try_task.is_err());

        // Fetch the maximum number of tasks permitted for a contributor.
        let contributor_lock_chunk_limit = environment.contributor_lock_chunk_limit();
        for i in 0..contributor_lock_chunk_limit {
            // Fetch a pending task for the contributor.
            let task = state.pop_task(&contributor).unwrap();
            let chunk_id = i as u64;
            assert_eq!((chunk_id, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor, chunk_id).unwrap();
            state
                .completed_task(&contributor, chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(i, state.pending_verification.len());
        }
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(contributor_lock_chunk_limit, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());

        // Fetch the maximum number of tasks permitted for a verifier.
        for i in 0..environment.verifier_lock_chunk_limit() {
            // Fetch a pending task for the verifier.
            let task = state.pop_task(&verifier).unwrap();
            assert_eq!((i as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&verifier, task.chunk_id).unwrap();
            state
                .completed_task(&verifier, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(contributor_lock_chunk_limit - i, state.pending_verification.len());
        }
        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(1, state.current_contributors.len());
        assert_eq!(1, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(0, state.finished_contributors.len());
        assert_eq!(0, state.finished_verifiers.len());

        // Attempt to fetch past the permitted lock chunk limit.
        for _ in 0..10 {
            let try_task = state.pop_task(&verifier);
            assert!(try_task.is_err());
        }
    }

    #[test]
    fn test_round_2x1() {
        test_logger();

        let environment = TEST_ENVIRONMENT.clone();

        // Fetch two contributors and two verifiers.
        let contributor_1 = TEST_CONTRIBUTOR_ID.clone();
        let contributor_2 = TEST_CONTRIBUTOR_ID_2.clone();
        let verifier = TEST_VERIFIER_ID.clone();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.set_current_round_height(current_round_height);
        state.add_to_queue(contributor_1.clone(), 10).unwrap();
        state.add_to_queue(contributor_2.clone(), 9).unwrap();
        state.add_to_queue(verifier.clone(), 10).unwrap();
        state.update_queue().unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        assert_eq!(3, state.queue.len());
        assert_eq!(0, state.next.len());
        state.precommit_next_round(next_round_height).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(3, state.next.len());
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());

        // Process every chunk in the round as contributor 1 and contributor 2.
        let offset = 4;
        let number_of_chunks = environment.number_of_chunks();
        for i in 0..number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(2, state.current_contributors.len());
            assert_eq!(1, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for contributor 1.
            let task = state.pop_task(&contributor_1).unwrap();
            let chunk_id = i as u64;
            assert_eq!((chunk_id as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor_1, task.chunk_id).unwrap();
            let assigned_verifier_1 = state
                .completed_task(&contributor_1, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());
            assert_eq!(verifier, assigned_verifier_1);

            // Fetch a pending task for contributor 2.
            let task = state.pop_task(&contributor_2).unwrap();
            let chunk_id_2 = (i as u64 + offset) % number_of_chunks;
            assert_eq!((chunk_id_2 as u64, 2), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor_2, task.chunk_id).unwrap();
            let assigned_verifier_2 = state
                .completed_task(&contributor_2, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(2, state.pending_verification.len());
            assert!(!state.is_current_round_finished());
            assert_eq!(assigned_verifier_1, assigned_verifier_2);

            // Fetch a pending task for the verifier.
            let task = state.pop_task(&verifier).unwrap();
            assert_eq!((chunk_id as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&verifier, task.chunk_id).unwrap();
            state
                .completed_task(&verifier, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.pop_task(&verifier).unwrap();
            assert_eq!((chunk_id_2 as u64, 2), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&verifier, task.chunk_id).unwrap();
            state
                .completed_task(&verifier, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

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
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(2, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(1, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }

    #[test]
    fn test_round_2x2() {
        test_logger();

        let environment = TEST_ENVIRONMENT.clone();

        // Fetch two contributors and two verifiers.
        let contributor_1 = TEST_CONTRIBUTOR_ID.clone();
        let contributor_2 = TEST_CONTRIBUTOR_ID_2.clone();
        let verifier_1 = TEST_VERIFIER_ID.clone();
        let verifier_2 = TEST_VERIFIER_ID_2.clone();

        // Initialize a new coordinator state.
        let current_round_height = 5;
        let mut state = CoordinatorState::new(environment.clone());
        state.set_current_round_height(current_round_height);
        state.add_to_queue(contributor_1.clone(), 10).unwrap();
        state.add_to_queue(contributor_2.clone(), 9).unwrap();
        state.add_to_queue(verifier_1.clone(), 10).unwrap();
        state.add_to_queue(verifier_2.clone(), 9).unwrap();
        state.update_queue().unwrap();
        assert!(state.is_current_round_finished());
        assert!(state.is_precommit_next_round_ready());

        // Advance the coordinator to the next round.
        let next_round_height = current_round_height + 1;
        assert_eq!(4, state.queue.len());
        assert_eq!(0, state.next.len());
        state.precommit_next_round(next_round_height).unwrap();
        assert_eq!(0, state.queue.len());
        assert_eq!(4, state.next.len());
        state.commit_next_round();
        assert_eq!(0, state.queue.len());
        assert_eq!(0, state.next.len());

        // Process every chunk in the round as contributor 1.
        let number_of_chunks = environment.number_of_chunks();
        for i in 0..number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(2, state.current_contributors.len());
            assert_eq!(2, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(0, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for the contributor.
            let task = state.pop_task(&contributor_1).unwrap();
            let chunk_id = i as u64;
            assert_eq!((chunk_id as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor_1, task.chunk_id).unwrap();
            let assigned_verifier = state
                .completed_task(&contributor_1, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.pop_task(&assigned_verifier).unwrap();
            assert_eq!((chunk_id as u64, 1), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&assigned_verifier, task.chunk_id).unwrap();
            state
                .completed_task(&assigned_verifier, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

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
        }

        // Process every chunk in the round as contributor 2.
        let offset = 4;
        for i in offset..offset + number_of_chunks {
            assert_eq!(Some(next_round_height), state.current_round_height);
            assert_eq!(1, state.current_contributors.len());
            assert_eq!(2, state.current_verifiers.len());
            assert_eq!(0, state.pending_verification.len());
            assert_eq!(1, state.finished_contributors.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.finished_verifiers.get(&next_round_height).unwrap().len());
            assert_eq!(0, state.dropped.len());
            assert_eq!(0, state.banned.len());

            // Fetch a pending task for the contributor.
            let task = state.pop_task(&contributor_2).unwrap();
            let chunk_id = i as u64 % number_of_chunks;
            assert_eq!((chunk_id, 2), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&contributor_2, task.chunk_id).unwrap();
            let assigned_verifier = state
                .completed_task(&contributor_2, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(1, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

            // Fetch a pending task for the verifier.
            let task = state.pop_task(&assigned_verifier).unwrap();
            assert_eq!((chunk_id, 2), (task.chunk_id, task.contribution_id));

            state.acquired_lock(&assigned_verifier, task.chunk_id).unwrap();
            state
                .completed_task(&assigned_verifier, task.chunk_id, task.contribution_id)
                .unwrap();
            assert_eq!(0, state.pending_verification.len());
            assert!(!state.is_current_round_finished());

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
        }

        assert_eq!(Some(next_round_height), state.current_round_height);
        assert_eq!(0, state.current_contributors.len());
        assert_eq!(0, state.current_verifiers.len());
        assert_eq!(0, state.pending_verification.len());
        assert_eq!(2, state.finished_contributors.get(&next_round_height).unwrap().len());
        assert_eq!(2, state.finished_verifiers.get(&next_round_height).unwrap().len());
        assert_eq!(0, state.dropped.len());
        assert_eq!(0, state.banned.len());
    }
}
