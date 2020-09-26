use crate::{objects::Chunk, CoordinatorError};

use chrono::{DateTime, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use tracing::info;

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
    duration_in_seconds: i64,
    contributor_ids: Vec<String>,
    verifier_ids: Vec<String>,
    chunks: Vec<Chunk>,
}

impl Round {
    /// Creates a new instance of `Ceremony`.
    #[inline]
    pub fn new(
        version: u64,
        height: u64,
        started_at: DateTime<Utc>,
        contributor_ids: &Vec<String>,
        verifier_ids: &Vec<String>,
        chunk_verifier_ids: &Vec<String>,
        chunk_verified_base_url: &Vec<&str>,
    ) -> Result<Self, CoordinatorError> {
        info!("Creating round {}", height);

        // Check that the chunk verifier IDs all exist in the list of verifier IDs.
        for id in chunk_verifier_ids {
            if !verifier_ids.contains(id) {
                return Err(CoordinatorError::MissingVerifierIds);
            }
        }

        // Derive the number of chunks based on the number of chunk verifier IDs.
        let num_chunks = chunk_verifier_ids.len();
        if num_chunks == 0 {
            return Err(CoordinatorError::InvalidNumberOfChunks);
        }

        // Check that the number of chunks matches with the number of base URLs given.
        if num_chunks != chunk_verified_base_url.len() {
            return Err(CoordinatorError::InvalidNumberOfChunks);
        }

        // Construct the chunks for this round.
        let chunks: Vec<Chunk> = (0..num_chunks)
            .into_par_iter()
            .map(|chunk_id| {
                Chunk::new(
                    chunk_id as u64,
                    chunk_verifier_ids[chunk_id].clone(),
                    chunk_verified_base_url[chunk_id].clone(),
                )
                .unwrap()
            })
            .collect();

        Ok(Self {
            version,
            height,
            started_at: Some(started_at),
            finished_at: None,
            duration_in_seconds: -1,
            contributor_ids: contributor_ids.clone(),
            verifier_ids: verifier_ids.clone(),
            chunks,
        })
    }

    /// Returns the height of the round.
    #[inline]
    pub fn get_height(&self) -> u64 {
        self.height
    }

    /// Returns `true` if the given participant ID is authorized as a contributor.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_authorized_contributor(&self, participant_id: String) -> bool {
        self.contributor_ids.contains(&participant_id)
    }

    /// Returns `true` if the given participant ID is authorized as a verifier.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_authorized_verifier(&self, participant_id: String) -> bool {
        self.verifier_ids.contains(&participant_id)
    }

    /// Returns a reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_chunk(&self, chunk_id: u64) -> Result<&Chunk, CoordinatorError> {
        match self.chunks.par_iter().find_any(|chunk| chunk.id() == chunk_id) {
            Some(chunk) => Ok(chunk),
            None => Err(CoordinatorError::MissingChunk),
        }
    }

    /// Returns a mutable reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_chunk_mut(&mut self, chunk_id: u64) -> Option<&mut Chunk> {
        self.chunks.par_iter_mut().find_any(|chunk| chunk.id() == chunk_id)
    }

    /// Returns a reference to a list of the chunks.
    #[inline]
    pub fn get_chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    /// Updates the chunk at a given chunk ID to a given updated chunk, if the chunk ID exists.
    #[inline]
    pub(crate) fn update_chunk(&mut self, chunk_id: u64, updated_chunk: &Chunk) -> bool {
        if self.chunks.par_iter().filter(|chunk| chunk.id() == chunk_id).count() == 1 {
            self.chunks = self
                .chunks
                .par_iter_mut()
                .map(|chunk| match chunk.id() == chunk_id {
                    true => updated_chunk,
                    false => chunk,
                })
                .cloned()
                .collect();
            true
        } else {
            false
        }
    }

    /// Returns a reference to a list of verifier IDs.
    #[inline]
    pub fn get_verifier_ids(&self) -> &Vec<String> {
        &self.verifier_ids
    }

    /// Returns `true` if all contributions in all chunks are verified.
    /// Otherwise, returns `false`.
    #[inline]
    pub fn are_chunks_verified(&self) -> bool {
        let num_contributors = self.contributor_ids.len() as u64;
        self.chunks
            .par_iter()
            .filter(|chunk| {
                let missing_contributions = chunk.contribution_id() < num_contributors;
                let is_not_verified = !chunk
                    .get_contributions()
                    .par_iter()
                    .filter(|contribution| !contribution.is_verified())
                    .collect::<Vec<_>>()
                    .is_empty();
                missing_contributions || is_not_verified
            })
            .collect::<Vec<_>>()
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::prelude::*;

    #[test]
    fn test_round_1_matches() {
        let expected = test_round_0().unwrap();
        let candidate = Round::new(
            TEST_VERSION, /* version */
            0,            /* height */
            *TEST_STARTED_AT,
            &TEST_CONTRIBUTOR_IDS,
            &TEST_VERIFIER_IDS,
            &TEST_CHUNK_VERIFIER_IDS,
            &TEST_CHUNK_VERIFIED_BASE_URLS,
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
        let round_1 = test_round_0().unwrap();
        assert!(round_1.is_authorized_contributor(TEST_CONTRIBUTOR_ID_1.to_string()));
    }

    #[test]
    fn test_is_authorized_verifier() {
        let round_1 = test_round_0().unwrap();
        assert!(round_1.is_authorized_verifier(TEST_VERIFIER_ID_1.to_string()));
    }

    #[test]
    fn test_get_chunk() {
        let expected = test_round_1_json().unwrap().chunks[0].clone();
        let candidate = test_round_0().unwrap().get_chunk(0).unwrap().clone();
        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_get_chunk_mut() {
        let mut expected = test_round_1_json().unwrap().chunks[0].clone();
        expected.acquire_lock("test_updated_contributor").unwrap();

        let mut candidate = test_round_0().unwrap().get_chunk_mut(0).unwrap().clone();
        candidate.acquire_lock("test_updated_contributor").unwrap();

        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_update_chunk() {
        let locked_chunk = {
            let mut locked_chunk = test_round_1_json().unwrap().chunks[0].clone();
            locked_chunk.acquire_lock("test_updated_contributor").unwrap();
            locked_chunk
        };

        let expected = {
            let mut expected = test_round_1_json().unwrap();
            expected.chunks[0] = locked_chunk.clone();
            expected
        };

        let mut candidate = test_round_0().unwrap();
        assert!(candidate.update_chunk(0, &locked_chunk));
        assert_eq!(expected, candidate);
    }

    #[test]
    fn test_get_verifier_ids() {
        let candidates = test_round_0().unwrap().get_verifier_ids().clone();
        for (index, id) in TEST_VERIFIER_IDS.iter().enumerate() {
            assert_eq!(*id, candidates[index]);
        }
    }

    #[test]
    fn test_are_chunks_verified() {
        // TODO (howardwu): Add tests for a full completeness check.
        let round = test_round_0().unwrap();
        assert!(!round.are_chunks_verified());
    }
}
