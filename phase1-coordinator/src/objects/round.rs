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
pub struct Round {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    version: u64,
    contributor_ids: Vec<String>,
    verifier_ids: Vec<String>,
    chunks: Vec<Chunk>,
}

impl Round {
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
    pub fn is_authorized(&self, participant_id: String) -> bool {
        if self.contributor_ids.contains(&participant_id) {
            return true;
        }
        if self.verifier_ids.contains(&participant_id) {
            return true;
        }
        false
    }

    #[inline]
    pub fn get_chunk(&self, chunk_id: u64) -> Option<&Chunk> {
        self.chunks.iter().filter(|chunk| chunk.chunk_id == chunk_id).next()
    }

    #[inline]
    fn get_chunk_mut(&mut self, chunk_id: u64) -> Option<&mut Chunk> {
        self.chunks.iter_mut().filter(|chunk| chunk.chunk_id == chunk_id).next()
    }

    #[inline]
    pub fn try_lock_chunk(&mut self, chunk_id: u64, participant_id: String) -> bool {
        if self
            .chunks
            .iter()
            .filter(|chunk| chunk.lock_holder == Some(participant_id.clone()))
            .next()
            .is_some()
        {
            error!(
                "Participant {} is already holding the lock on chunk {}",
                participant_id, chunk_id
            );
            // TODO (howardwu): Revisit this return value.
            return true;
        }

        let verifier_ids = self.verifier_ids.clone();

        let mut chunk = match self.get_chunk_mut(chunk_id) {
            Some(chunk) => chunk,
            None => return false,
        };
        if chunk.lock_holder.is_some() {
            return false;
        }

        // Return false if contributor trying to lock unverified chunk or
        // if verifier trying to lock verified chunk.
        if let Some(contribution) = chunk.contributions.last() {
            let is_verified = verifier_ids.contains(&participant_id);
            if contribution.verified == is_verified {
                return false;
            }
        }

        chunk.lock_holder = Some(participant_id);
        true
    }

    #[inline]
    pub fn contribute_chunk(&mut self, chunk_id: u64, participant_id: String, location: Url) -> bool {
        let verifier_ids = self.verifier_ids.clone();

        let mut chunk = match self.get_chunk_mut(chunk_id) {
            Some(chunk) => chunk,
            None => return false,
        };

        if chunk.lock_holder != Some(participant_id.clone()) {
            error!(
                "Participant {} does not hold lock on chunk {}",
                participant_id, chunk_id
            );
            return false;
        }

        let index = chunk.contributions.len() - 1;

        match verifier_ids.contains(&participant_id) {
            true => match chunk.contributions.get_mut(index) {
                Some(contribution) => {
                    contribution.verifier_id = Some(participant_id);
                    contribution.verified_location = Some(location);
                    contribution.verified = true;
                }
                None => return false,
            },
            false => chunk.contributions.push(Contribution {
                contributor_id: Some(participant_id),
                contributed_location: Some(location),
                verifier_id: None,
                verified_location: None,
                verified: false,
            }),
        }

        chunk.lock_holder = None;
        true
    }
}
