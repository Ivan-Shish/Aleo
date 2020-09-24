use crate::objects::Contribution;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    chunk_id: u64,
    lock_holder: Option<String>,
    contributions: Vec<Contribution>,
}

impl Chunk {
    /// Returns the assigned chunk ID.
    #[inline]
    pub fn id(&self) -> u64 {
        self.chunk_id
    }

    /// Returns `true` if the current chunk is locked. Otherwise, returns `false`.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.lock_holder.is_some()
    }

    /// Returns `true` if the current chunk is locked by the given contributor ID.
    /// Otherwise, returns `false`.
    #[inline]
    pub fn is_locked_by(&self, participant_id: &str) -> bool {
        match &self.lock_holder {
            Some(lock_holder) => lock_holder == participant_id,
            None => false,
        }
    }

    /// Returns the last contribution in this chunk, if it exists. Otherwise, returns `false`.
    #[inline]
    pub fn get_latest_contribution(&self) -> Option<&Contribution> {
        self.contributions.last()
    }

    /// Returns `true` if the participant does not already hold the lock.
    /// Otherwise, returns `false`.
    #[inline]
    pub fn acquire_lock(&mut self, participant_id: &str) -> bool {
        // Check that the participant has not already contributed before.
        let matches: Vec<_> = self
            .contributions
            .par_iter()
            .filter(|contribution| *contribution.get_contributor_id() == Some(participant_id.to_string()))
            .collect();
        if !matches.is_empty() {
            return false;
        }

        // If the lock is currently held by the participant,
        // the latest contributor ID is the participant,
        // the latest contributed location is empty,
        // and the latest contribution is not verified,
        // then it could mean the contributor lost
        // connection and is attempting to reconnect.
        //
        // In this case, no further action needs to be taken,
        // and we may return true.
        let contribution = match self.get_latest_contribution() {
            Some(contribution) => contribution,
            _ => return false,
        };

        if self.is_locked_by(participant_id)
            && *contribution.get_contributor_id() == Some(participant_id.to_string())
            && contribution.get_contributed_location().is_none()
            && !contribution.is_verified()
        {
            return true;
        }

        self.lock_holder = Some(participant_id.to_string());
        true

        // Return `false` if a contributor trying to lock an unverified chunk
        // or if a verifier is trying to lock a verified chunk.
        // if let Some(contribution) = self.get_latest_contribution() {
        //     // Case 1 - Contributor is attempting to lock an unverified chunk.
        //     // let is_verified = current_round.get_verifier_ids().contains(&participant_id);
        //     // if contribution.verified == is_verified {
        //     //     return Err(CoordinatorError::UnauthorizedChunkContributor);
        //     // }
        // }
    }

    /// Generates an unique number representing the current state of the chunk.
    #[inline]
    pub fn version(&self) -> u32 {
        let matching_contributions: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.get_contributor_id().is_some() as u32)
            .sum();
        let matching_verifications: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.get_verifier_id().is_some() as u32)
            .sum();
        return matching_contributions + matching_verifications;
    }
}
