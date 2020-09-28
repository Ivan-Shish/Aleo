use crate::{
    environment::Environment,
    objects::{participant::*, Chunk},
    CoordinatorError,
};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use tracing::{debug, error, trace};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
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
    pub fn new(
        environment: &Environment,
        height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: Vec<Participant>,
        verifier_ids: Vec<Participant>,
        chunk_verifier_ids: Vec<Participant>,
    ) -> Result<Self, CoordinatorError> {
        debug!("Creating round {}", height);

        // Check that the contributor correspond to the contributor participant type.
        let num_noncontributors = contributor_ids
            .par_iter()
            .filter(|participant_id| !participant_id.is_contributor())
            .count();
        if num_noncontributors > 0 {
            error!("Found {} participants who are not contributors", num_noncontributors);
            return Err(CoordinatorError::ExpectedContributor);
        }
        // Check that the verifier correspond to the verifier participant type.
        if verifier_ids
            .par_iter()
            .filter(|participant_id| !participant_id.is_verifier())
            .count()
            > 0
        {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Fetch the version number and number of chunks from the given environment.
        let version = environment.version();
        let num_chunks = environment.number_of_chunks();

        // Check that the number of chunks is nonzero.
        if num_chunks == 0 {
            return Err(CoordinatorError::NumberOfChunksInvalid);
        }
        // Check that the number of chunks matches the given number of chunk verifier IDs.
        if num_chunks != chunk_verifier_ids.len() as u64 {
            return Err(CoordinatorError::NumberOfChunkVerifierIdsInvalid);
        }

        // Check that the chunk verifier IDs all exist in the list of verifier IDs.
        for id in &chunk_verifier_ids {
            if !verifier_ids.contains(id) {
                return Err(CoordinatorError::MissingVerifierIds);
            }
        }

        // Construct the chunks for this round.
        let chunks: Vec<Chunk> = (0..num_chunks as usize)
            .into_par_iter()
            .zip(chunk_verifier_ids)
            .map(|(chunk_id, verifier)| {
                Chunk::new(
                    chunk_id as u64,
                    verifier.clone(),
                    environment.contribution_locator(height, chunk_id as u64, 0),
                )
                .expect("failed to create chunk")
            })
            .collect();

        debug!("Created round {}", height);

        Ok(Self {
            version,
            height,
            started_at: Some(started_at),
            finished_at: None,
            contributor_ids,
            verifier_ids,
            chunks,
        })
    }

    /// Returns the version number set in the round.
    #[inline]
    pub fn get_version(&self) -> u64 {
        self.version
    }

    /// Returns the height of the round.
    #[inline]
    pub fn get_height(&self) -> u64 {
        self.height
    }

    /// Returns `true` if the given participant is authorized as a contributor.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_authorized_contributor(&self, participant: &Participant) -> bool {
        self.contributor_ids.contains(participant)
    }

    /// Returns `true` if the given participant is authorized as a verifier.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_authorized_verifier(&self, participant: &Participant) -> bool {
        self.verifier_ids.contains(participant)
    }

    /// Returns the number of contributors, and therefore,
    /// the number of expected contributions for this round.
    #[inline]
    pub fn num_contributors(&self) -> u64 {
        self.contributor_ids.len() as u64
    }

    /// Returns a reference to a list of verifiers.
    #[inline]
    pub fn get_verifiers(&self) -> &Vec<Participant> {
        &self.verifier_ids
    }

    /// Returns a reference to a list of the chunks.
    #[inline]
    pub fn get_chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    /// Returns a reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_chunk(&self, chunk_id: u64) -> Result<&Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let mut chunk = match self.chunks.get(chunk_id as usize) {
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
    pub fn get_chunk_mut(&mut self, chunk_id: u64) -> Result<&mut Chunk, CoordinatorError> {
        // Fetch the chunk with the given chunk ID.
        let mut chunk = match self.chunks.get_mut(chunk_id as usize) {
            Some(chunk) => chunk,
            _ => return Err(CoordinatorError::ChunkMissing),
        };

        // Check the ID in the chunk matches the given chunk ID.
        match chunk.chunk_id() == chunk_id {
            true => Ok(chunk),
            false => Err(CoordinatorError::ChunkIdMismatch),
        }
    }

    /// Updates the chunk at a given chunk ID to a given updated chunk, if the chunk ID exists.
    #[inline]
    pub(crate) fn set_chunk(&mut self, chunk_id: u64, updated_chunk: Chunk) -> Result<(), CoordinatorError> {
        let mut chunk = self.get_chunk_mut(chunk_id)?;
        *chunk = updated_chunk;
        Ok(())
    }

    /// Returns `true` if the chunk corresponding to the given chunk ID is
    /// locked by the given participant. Otherwise, returns `false`.
    #[inline]
    pub fn is_chunk_locked_by(&self, chunk_id: u64, participant: &Participant) -> bool {
        match self.get_chunk(chunk_id) {
            Ok(chunk) => chunk.is_locked_by(participant),
            _ => false,
        }
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID from storage
    /// for a given participant.
    ///
    #[inline]
    pub fn try_lock_chunk(&mut self, chunk_id: u64, participant: Participant) -> Result<(), CoordinatorError> {
        // If the participant is a contributor ID, check they are authorized to acquire the lock as a contributor.
        if participant.is_contributor() {
            // Check that the contributor is an authorized contributor in this round.
            if !self.is_authorized_contributor(&participant) {
                error!("{} is not an authorized contributor", &participant);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the contributor does not currently hold a lock to any chunk.
            if self
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
            // Check that the verifier is an authorized verifier in this round.
            if !self.is_authorized_verifier(&participant) {
                error!("{} is not an authorized verifier", &participant);
                return Err(CoordinatorError::UnauthorizedChunkVerifier);
            }
        }

        // Attempt to acquire the lock for the given participant ID.
        let num_contributors = self.num_contributors();
        self.get_chunk_mut(chunk_id)?
            .acquire_lock(participant.clone(), num_contributors)?;

        debug!("{} acquired lock on chunk {}", participant, chunk_id);
        Ok(())
    }

    ///
    /// Updates the contribution corresponding to a given chunk ID and
    /// contribution ID as verified.
    ///
    /// This function assumes the current contribution already has
    /// a verifier assigned to it.
    ///
    #[inline]
    pub fn verify_contribution(
        &mut self,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
    ) -> Result<(), CoordinatorError> {
        // Set the current contribution as verified for the given chunk ID.
        self.get_chunk_mut(chunk_id)?
            .verify_contribution(contribution_id, participant)?;

        // If the chunk is complete and the finished at timestamp has not been set yet,
        // then set it with the current UTC timestamp.
        if self.is_complete() && self.finished_at.is_none() {
            self.finished_at = Some(Utc::now());
        }

        Ok(())
    }

    /// Returns `true` if all contributions in all chunks are verified.
    /// Otherwise, returns `false`.
    #[inline]
    pub fn is_complete(&self) -> bool {
        let num_contributors = self.num_contributors();
        self.chunks
            .par_iter()
            .filter(|chunk| !chunk.is_complete(num_contributors))
            .collect::<Vec<_>>()
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::prelude::*;

    #[test]
    fn test_round_0_matches() {
        let expected = test_round_0().unwrap();
        let candidate = Round::new(
            &TEST_ENVIRONMENT,
            0, /* height */
            *TEST_STARTED_AT,
            TEST_CONTRIBUTOR_IDS.to_vec(),
            TEST_VERIFIER_IDS.to_vec(),
            TEST_CHUNK_VERIFIER_IDS.to_vec(),
        )
        .unwrap();

        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }

    #[test]
    fn test_get_height() {
        let round = test_round_0().unwrap();
        assert_eq!(0, round.get_height());
    }

    #[test]
    fn test_is_authorized_contributor() {
        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_authorized_contributor(&TEST_CONTRIBUTOR_ID_1));
    }

    #[test]
    fn test_is_authorized_verifier() {
        let round_0 = test_round_0().unwrap();
        assert!(round_0.is_authorized_verifier(&TEST_VERIFIER_ID_1));
    }

    #[test]
    fn test_get_chunk() {
        let expected = test_round_0_json().unwrap().chunks[0].clone();
        let candidate = test_round_0().unwrap().get_chunk(0).unwrap().clone();
        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_get_chunk_mut() {
        let mut expected = test_round_0_json().unwrap().chunks[0].clone();
        expected
            .acquire_lock(Participant::Contributor("test_updated_contributor".to_string()), 1)
            .unwrap();

        let mut candidate = test_round_0().unwrap().get_chunk_mut(0).unwrap().clone();
        candidate
            .acquire_lock(Participant::Contributor("test_updated_contributor".to_string()), 1)
            .unwrap();

        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_set_chunk() {
        let locked_chunk = {
            let mut locked_chunk = test_round_0_json().unwrap().chunks[0].clone();
            locked_chunk
                .acquire_lock(Participant::Contributor("test_updated_contributor".to_string()), 1)
                .unwrap();
            locked_chunk
        };

        let expected = {
            let mut expected = test_round_0_json().unwrap();
            expected.chunks[0] = locked_chunk.clone();
            expected
        };

        let mut candidate = test_round_0().unwrap();
        assert!(candidate.set_chunk(0, locked_chunk).is_ok());
        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_get_verifiers() {
        let candidates = test_round_0().unwrap().get_verifiers().clone();
        for (index, id) in TEST_VERIFIER_IDS.iter().enumerate() {
            assert_eq!(*id, candidates[index]);
        }
    }

    #[test]
    fn test_are_chunks_verified() {
        // TODO (howardwu): Add tests for a full completeness check.
        let round = test_round_0().unwrap();
        assert!(!round.is_complete());
    }
}
