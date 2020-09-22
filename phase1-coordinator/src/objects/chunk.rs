use rayon::prelude::*;
use serde::{
    de::{self, Deserializer},
    Deserialize,
    Serialize,
};
use serde_aux::prelude::*;
use std::{fmt::Display, str::FromStr};
use url::{ParseError, Url};
use url_serde;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    chunk_id: u64,
    lock_holder: Option<String>,
    contributions: Vec<Contribution>,
}

impl Chunk {
    /// Generates an unique number representing the current state of the chunk.
    #[inline]
    pub fn version(&self) -> u32 {
        let matching_contributions: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.contributor_id.is_some() as u32)
            .sum();
        let matching_verifications: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.verifier_id.is_some() as u32)
            .sum();
        return matching_contributions + matching_verifications;
    }
}
