use crate::{
    objects::{Chunk, Contribution},
    CoordinatorError,
};

use rayon::prelude::*;
use serde::{
    de::{self, Deserializer},
    Deserialize,
    Serialize,
};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use tracing::{error, info};
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct Round {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    version: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    height: u64,
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

    /// Sets the chunk at the given chunk ID in the round to the given, updated chunk.
    #[inline]
    pub(crate) fn set_chunk(&mut self, chunk_id: u64, updated_chunk: &Chunk) -> bool {
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

    /// Returns a reference to the chunk, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_chunk(&self, chunk_id: u64) -> Option<&Chunk> {
        self.chunks.par_iter().find_any(|chunk| chunk.id() == chunk_id)
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

    /// Returns a reference to a list of verifier IDs.
    #[inline]
    pub fn get_verifier_ids(&self) -> &Vec<String> {
        &self.verifier_ids
    }

    /// Returns `true` if the current round has been completed and verified.
    #[inline]
    pub fn is_complete(&self) -> bool {
        let num_contributions_total = self.contributor_ids.len();
        self.chunks
            .par_iter()
            .filter(|chunk| {
                let missing_contributions = chunk.num_contributions() < num_contributions_total;
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

    // TODO (howardwu): Rename to set_round.
    /// Set the round to the new round.
    #[inline]
    fn set_ceremony(&mut self, new_round: Self) {
        if self.version != new_round.version {
            error!("New ceremony is outdated ({} vs {})", self.version, new_round.version);
        } else {
            // Set self to new version.
            *self = new_round;
        }
    }
}
