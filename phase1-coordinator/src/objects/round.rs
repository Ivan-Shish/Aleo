use crate::{
    environment::Environment,
    objects::{participant::*, Chunk},
    Coordinator,
    CoordinatorError,
};

use crate::storage::StorageWriter;
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use std::{collections::HashSet, hash::Hash};
use tracing::{debug, error, trace};

/// A helper function used to check that each list of participants is unique.
fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct Round {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    version: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    height: u64,
    #[serde_diff(opaque)]
    started_at: Option<DateTime<Utc>>,
    #[serde_diff(opaque)]
    finished_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_contributors_from_strings")]
    contributor_ids: Vec<Participant>,
    #[serde(deserialize_with = "deserialize_verifiers_from_strings")]
    verifier_ids: Vec<Participant>,
    chunks: Vec<Chunk>,
}

impl Round {
    /// Creates a new instance of `Round`.
    #[inline]
    pub(crate) fn new(
        environment: &Environment,
        storage: &StorageWriter,
        round_height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: Vec<Participant>,
        verifier_ids: Vec<Participant>,
    ) -> Result<Self, CoordinatorError> {
        debug!("Starting to create round {}", round_height);
        {
            // Check that the list of contributor IDs is not empty.
            // This check is only enforced if the round height is not 0.
            if contributor_ids.is_empty() && round_height != 0 {
                return Err(CoordinatorError::RoundContributorsMissing);
            }
            // Check that each contributor ID is unique.
            if !has_unique_elements(&contributor_ids) {
                return Err(CoordinatorError::RoundContributorsNotUnique);
            }
            // Check that each contributor ID is a contributor participant type.
            let num_contributors = contributor_ids
                .par_iter()
                .filter(|id| Participant::is_contributor(id))
                .count();
            if num_contributors != contributor_ids.len() {
                error!("{} IDs are not contributors", contributor_ids.len() - num_contributors);
                return Err(CoordinatorError::ExpectedContributor);
            }
        }
        {
            // Check that the list of verifier IDs is not empty.
            if verifier_ids.is_empty() {
                return Err(CoordinatorError::RoundVerifiersMissing);
            }
            // Check that each verifier ID is unique.
            if !has_unique_elements(&verifier_ids) {
                return Err(CoordinatorError::RoundVerifiersNotUnique);
            }
            // Check that each verifier ID is a verifier participant type.
            let num_verifiers = verifier_ids
                .par_iter()
                .filter(|id| Participant::is_verifier(id))
                .count();
            if num_verifiers != verifier_ids.len() {
                error!("{} IDs are not verifiers", verifier_ids.len() - num_verifiers);
                return Err(CoordinatorError::ExpectedVerifier);
            }
        }

        // Check that the number of chunks is nonzero.
        if environment.number_of_chunks() == 0 {
            return Err(CoordinatorError::NumberOfChunksInvalid);
        }

        // Construct the chunks for this round.
        //
        // Initialize the chunk verifiers as a list comprising only
        // the coordinator verifier, as this is for initialization.
        let chunks: Vec<Chunk> = (0..environment.number_of_chunks() as usize)
            .into_par_iter()
            .map(|chunk_id| {
                Chunk::new(
                    chunk_id as u64,
                    environment.coordinator_verifier(),
                    storage.contribution_locator(round_height, chunk_id as u64, 0, true),
                )
                .expect("failed to create chunk")
            })
            .collect();

        debug!("Completed creating round {}", round_height);

        Ok(Self {
            version: environment.version(),
            height: round_height,
            started_at: Some(started_at),
            finished_at: None,
            contributor_ids,
            verifier_ids,
            chunks,
        })
    }

    /// Returns the version number set in the round.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the height of the round.
    #[inline]
    pub fn round_height(&self) -> u64 {
        self.height
    }

    /// Returns the number of contributors authorized for this round.
    #[inline]
    pub fn number_of_contributors(&self) -> u64 {
        self.contributor_ids.len() as u64
    }

    /// Returns the number of verifiers authorized for this round.
    #[inline]
    pub fn number_of_verifiers(&self) -> u64 {
        self.verifier_ids.len() as u64
    }

    /// Returns the expected number of contributions.
    pub fn expected_number_of_contributions(&self) -> u64 {
        // The expected number of contributions is one more than
        // the total number of authorized contributions to account
        // for the initialization contribution in each round.
        self.number_of_contributors() + 1
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// contributor and listed in the contributor IDs for this round.
    ///
    /// If the participant is not a contributor, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_authorized_contributor(&self, participant: &Participant) -> bool {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return false;
        }
        // Check that the participant is not a verifier.
        if participant.is_verifier() {
            return false;
        }
        // Check that the participant is a contributor for the given round height.
        self.contributor_ids.contains(participant)
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// verifier and listed in the verifier IDs for this round.
    ///
    /// If the participant is not a verifier, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_authorized_verifier(&self, participant: &Participant) -> bool {
        // Check that the participant is not a contributor.
        if participant.is_contributor() {
            return false;
        }
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return false;
        }
        // Check that the participant is a verifier for the given round height.
        self.verifier_ids.contains(participant)
    }

    ///
    /// Returns the current contribution locator for a given chunk ID.
    ///
    /// If the current contribution is NOT contributed yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the current contribution is already verified,
    /// this function will return a `CoordinatorError`.
    ///
    #[inline]
    pub fn current_contribution_locator(
        &self,
        storage: &StorageWriter,
        chunk_id: u64,
        verified: bool,
    ) -> Result<String, CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.get_chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let current_contribution_id = chunk.current_contribution_id();

        // Check that the contribution locator corresponding to the current contribution ID
        // exists for the current round and given chunk ID.
        if !storage.contribution_locator_exists(current_round_height, chunk_id, current_contribution_id, verified) {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Check that the current contribution ID is NOT verified yet.
        if chunk.get_contribution(current_contribution_id)?.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch the current contribution locator.
        let current_contribution_locator =
            storage.contribution_locator(current_round_height, chunk_id, current_contribution_id, verified);

        Ok(current_contribution_locator)
    }

    ///
    /// Returns the next contribution locator for a given chunk ID.
    ///
    /// If the current contribution is NOT contributed yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the current contribution is NOT verified yet,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the next contribution locator already exists,
    /// this function will return a `CoordinatorError`.
    ///
    /// If the chunk corresponding to the given chunk ID
    /// is already completed for the current round,
    /// this function will return a `CoordinatorError`.
    ///
    #[inline]
    pub fn next_contribution_locator(
        &self,
        storage: &StorageWriter,
        chunk_id: u64,
    ) -> Result<String, CoordinatorError> {
        // Fetch the current round height.
        let current_round_height = self.round_height();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.get_chunk(chunk_id)?;
        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = self.expected_number_of_contributions();
        // Fetch the next contribution ID.
        let next_contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the current contribution has been verified.
        if !chunk.current_contribution()?.is_verified() {
            return Err(CoordinatorError::ContributionMissingVerification);
        }

        // Check that the contribution locator corresponding to the next contribution ID
        // does NOT exist for the current round and given chunk ID.
        if storage.contribution_locator_exists(current_round_height, chunk_id, next_contribution_id, false) {
            return Err(CoordinatorError::ContributionLocatorAlreadyExists);
        }

        // Fetch the next contribution locator.
        let next_contribution_locator =
            storage.contribution_locator(current_round_height, chunk_id, next_contribution_id, false);

        Ok(next_contribution_locator)
    }

    /// Returns a reference to a list of verifiers.
    #[inline]
    pub(crate) fn get_verifiers(&self) -> &Vec<Participant> {
        &self.verifier_ids
    }

    /// Returns `true` if the chunk corresponding to the given chunk ID is
    /// locked by the given participant. Otherwise, returns `false`.
    #[inline]
    pub(crate) fn is_chunk_locked_by(&self, chunk_id: u64, participant: &Participant) -> bool {
        match self.get_chunk(chunk_id) {
            Ok(chunk) => chunk.is_locked_by(participant),
            _ => false,
        }
    }

    /// Returns a reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub(crate) fn get_chunk(&self, chunk_id: u64) -> Result<&Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let chunk = match self.chunks.get(chunk_id as usize) {
            Some(chunk) => chunk,
            _ => return Err(CoordinatorError::ChunkMissing),
        };

        // Check the ID in the chunk matches the given chunk ID.
        match chunk.chunk_id() == chunk_id {
            true => Ok(chunk),
            false => Err(CoordinatorError::ChunkIdMismatch),
        }
    }

    /// Returns a mutable reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub(crate) fn get_chunk_mut(&mut self, chunk_id: u64) -> Result<&mut Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let chunk = match self.chunks.get_mut(chunk_id as usize) {
            Some(chunk) => chunk,
            _ => return Err(CoordinatorError::ChunkMissing),
        };

        // Check the ID in the chunk matches the given chunk ID.
        match chunk.chunk_id() == chunk_id {
            true => Ok(chunk),
            false => Err(CoordinatorError::ChunkIdMismatch),
        }
    }

    /// Returns a reference to a list of the chunks.
    #[inline]
    pub(crate) fn get_chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID from storage
    /// for a given participant.
    ///
    #[inline]
    pub(crate) fn try_lock_chunk(
        &mut self,
        environment: &Environment,
        storage: &StorageWriter,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<String, CoordinatorError> {
        // Check that the participant is authorized to acquire the lock
        // associated with the given chunk ID for the current round,
        // and fetch the appropriate contribution locator.
        let contribution_locator = match participant {
            Participant::Contributor(_) => {
                // Check that the participant is an authorized contributor
                // for the current round.
                if !self.is_authorized_contributor(participant) {
                    error!("{} is not an authorized contributor", participant);
                    return Err(CoordinatorError::UnauthorizedChunkContributor);
                }
                // This call enforces a strict check that the
                // next contribution locator does NOT exist and
                // that the current contribution locator exists
                // and has already been verified.
                self.next_contribution_locator(storage, chunk_id)?
            }
            Participant::Verifier(_) => {
                // Check that the participant is an authorized verifier
                // for the current round.
                if !self.is_authorized_verifier(participant) {
                    error!("{} is not an authorized verifier", participant);
                    return Err(CoordinatorError::UnauthorizedChunkVerifier);
                }
                // This call enforces a strict check that the
                // current contribution locator exist and
                // has not been verified yet.
                self.current_contribution_locator(storage, chunk_id, false)?
            }
        };

        // Check that the participant is holding less than the chunk lock limit.
        let number_of_locks_held = self
            .chunks
            .par_iter()
            .filter(|chunk| chunk.is_locked_by(participant))
            .count();
        match participant {
            Participant::Contributor(_) => {
                if number_of_locks_held >= environment.contributor_lock_chunk_limit() {
                    trace!("{} chunks are locked by {}", &number_of_locks_held, participant);
                    return Err(CoordinatorError::ChunkLockLimitReached);
                }
            }
            Participant::Verifier(_) => {
                if number_of_locks_held >= environment.verifier_lock_chunk_limit() {
                    trace!("{} chunks are locked by {}", &number_of_locks_held, participant);
                    return Err(CoordinatorError::ChunkLockLimitReached);
                }
            }
        };

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = self.get_chunk(chunk_id)?;
        // Fetch the next contribution ID.
        let current_contribution = chunk.current_contribution()?;

        // As a corollary, if the current contribution locator exists
        // and the current contribution has not been verified yet,
        // check that the given participant is not a contributor.
        if self.current_contribution_locator(storage, chunk_id, false).is_ok() && !current_contribution.is_verified() {
            // Check that the given participant is not a contributor.
            if participant.is_contributor() {
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }
        }

        // Attempt to acquire the lock for the given participant ID.
        let expected_num_contributions = self.expected_number_of_contributions();
        self.get_chunk_mut(chunk_id)?
            .acquire_lock(participant.clone(), expected_num_contributions)?;

        debug!("{} acquired lock on chunk {}", participant, chunk_id);
        Ok(contribution_locator)
    }

    ///
    /// Updates the contribution corresponding to a given chunk ID and
    /// contribution ID as verified.
    ///
    /// This function assumes the current contribution already has
    /// a verifier assigned to it.
    ///
    #[inline]
    pub(crate) fn verify_contribution(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
        participant: Participant,
        verified_locator: String,
    ) -> Result<(), CoordinatorError> {
        // Set the current contribution as verified for the given chunk ID.
        self.get_chunk_mut(chunk_id)?
            .verify_contribution(contribution_id, participant, verified_locator)?;

        // If the chunk is complete and the finished at timestamp has not been set yet,
        // then set it with the current UTC timestamp.
        if self.is_complete() && self.finished_at.is_none() {
            self.finished_at = Some(Utc::now());
        }

        Ok(())
    }

    ///
    /// Returns `true` if all chunks are unlocked and all contributions in all chunks
    /// have been verified. Otherwise, returns `false`.
    ///
    #[inline]
    pub(crate) fn is_complete(&self) -> bool {
        // Check that all chunks are unlocked.
        let number_of_locks_held = self.chunks.par_iter().filter(|chunk| chunk.is_locked()).count();
        if number_of_locks_held > 0 {
            trace!("{} chunks are locked in round {}", &number_of_locks_held, self.height);
            return false;
        }

        // Check that all contributions in all chunks have been verified.
        let expected_num_contributions = self.expected_number_of_contributions();
        self.chunks
            .par_iter()
            .filter(|chunk| !chunk.is_complete(expected_num_contributions))
            .collect::<Vec<_>>()
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::prelude::*;

    #[test]
    #[serial]
    fn test_round_0_matches() {
        // Define test storage.
        let test_storage = test_storage();
        let storage = test_storage.write().unwrap();

        let expected = test_round_0().unwrap();
        let candidate = Round::new(
            &TEST_ENVIRONMENT,
            &storage,
            0, /* height */
            *TEST_STARTED_AT,
            vec![],
            TEST_VERIFIER_IDS.to_vec(),
        )
        .unwrap();

        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }

    #[test]
    #[serial]
    fn test_round_height() {
        let round_0 = test_round_0_json().unwrap();
        assert_eq!(0, round_0.round_height());

        let round_0 = test_round_0().unwrap();
        assert_eq!(0, round_0.round_height());

        let round_1 = test_round_1_initial_json().unwrap();
        assert_eq!(1, round_1.round_height());
    }

    #[test]
    #[serial]
    fn test_is_authorized_contributor() {
        let round_1 = test_round_1_initial_json().unwrap();
        assert!(round_1.is_authorized_contributor(&TEST_CONTRIBUTOR_ID));
    }

    #[test]
    #[serial]
    fn test_is_authorized_verifier() {
        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_authorized_verifier(&TEST_VERIFIER_ID));

        let round_1 = test_round_1_initial_json().unwrap();
        assert!(round_1.is_authorized_contributor(&TEST_CONTRIBUTOR_ID));
    }

    #[test]
    #[serial]
    fn test_get_chunk() {
        let expected = test_round_0_json().unwrap().chunks[0].clone();
        let candidate = test_round_0().unwrap().get_chunk(0).unwrap().clone();
        print_diff(&expected, &candidate);
        assert_eq!(expected, candidate);
    }

    #[test]
    #[serial]
    fn test_get_chunk_mut_basic() {
        let expected = test_round_0_json().unwrap().chunks[0].clone();
        let candidate = test_round_0().unwrap().get_chunk_mut(0).unwrap().clone();
        print_diff(&expected, &candidate);
        assert_eq!(expected, candidate);
    }

    #[test]
    #[serial]
    fn test_get_verifiers() {
        let candidates = test_round_0().unwrap().get_verifiers().clone();
        for (index, id) in TEST_VERIFIER_IDS.iter().enumerate() {
            assert_eq!(*id, candidates[index]);
        }
    }

    #[test]
    #[serial]
    fn test_is_complete() {
        // TODO (howardwu): Add tests for a full completeness check.
        let round_0 = test_round_0_json().unwrap();
        assert!(round_0.is_complete());

        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_complete());

        let round_1 = test_round_1_initial_json().unwrap();
        assert!(!round_1.is_complete());
    }
}
