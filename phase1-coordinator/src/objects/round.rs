use crate::objects::{Chunk, Contribution};

use rayon::prelude::*;
use serde::{
    de::{self, Deserializer},
    Deserialize,
    Serialize,
};
use serde_aux::prelude::*;
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Round {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    version: u64,
    contributor_ids: Vec<String>,
    verifier_ids: Vec<String>,
    chunks: Vec<Chunk>,
}

impl Round {
    /// Creates a new instance of `Ceremony`.
    #[inline]
    pub fn new(version: u64, contributor_ids: &Vec<String>, verifier_ids: &Vec<String>, chunks: &Vec<Chunk>) -> Self {
        Self {
            version,
            contributor_ids: contributor_ids.clone(),
            verifier_ids: verifier_ids.clone(),
            chunks: chunks.clone(),
        }
    }

    // TODO (howardwu): Rename to set_round.
    #[inline]
    fn set_ceremony(&mut self, new_round: Self) {
        if self.version != new_round.version {
            error!("New ceremony is out of date: {} vs {}", self.version, new_round.version);
        }
        // Set self to new version
        *self = new_round;
    }

    #[inline]
    pub fn is_authorized_contributor(&self, participant_id: String) -> bool {
        self.contributor_ids.contains(&participant_id)
    }

    #[inline]
    pub fn is_authorized_verifier(&self, participant_id: String) -> bool {
        self.verifier_ids.contains(&participant_id)
    }

    #[inline]
    pub fn set_chunk(&mut self, chunk_id: u64, updated_chunk: &Chunk) -> bool {
        if self.chunks.par_iter().filter(|chunk| chunk.id() == chunk_id).count() == 1 {
            let chunks: Vec<_> = self
                .chunks
                .par_iter_mut()
                .map(|chunk| match chunk.id() == chunk_id {
                    true => updated_chunk,
                    false => chunk,
                })
                .cloned()
                .collect();
            self.chunks = chunks;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn get_chunk(&self, chunk_id: u64) -> Option<&Chunk> {
        self.chunks.par_iter().find_any(|chunk| chunk.id() == chunk_id)
    }

    #[inline]
    pub fn get_chunk_mut(&mut self, chunk_id: u64) -> Option<&mut Chunk> {
        self.chunks.par_iter_mut().find_any(|chunk| chunk.id() == chunk_id)
    }

    #[inline]
    pub fn get_chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    #[inline]
    pub fn get_verifier_ids(&self) -> &Vec<String> {
        &self.verifier_ids
    }
}
