use crate::{objects::Contribution, CoordinatorError};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    chunk_id: u64,
    lock_holder: Option<String>,
    #[serde_diff(opaque)]
    contributions: Vec<Contribution>,
}

impl Chunk {
    /// Creates a new instance of `Chunk`.
    #[inline]
    pub fn new(chunk_id: u64, verifier_id: String, verifier_base_url: &str) -> Result<Self, CoordinatorError> {
        // Construct the starting contribution template for this chunk.
        let contribution = Contribution::new_verifier(chunk_id, 0, verifier_id, verifier_base_url)?;
        Ok(Self {
            chunk_id,
            lock_holder: None,
            contributions: vec![contribution],
        })
    }

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

    /// Returns the current number of contributions in this chunk,
    /// irrespective of the state of each contribution.
    #[inline]
    pub fn contribution_id(&self) -> u64 {
        self.contributions.len() as u64
    }

    /// Returns a reference to a list of contributions in this chunk.
    #[inline]
    pub fn get_contributions(&self) -> &Vec<Contribution> {
        &self.contributions
    }

    /// Returns the last contribution in this chunk, if it exists. Otherwise, returns `false`.
    #[inline]
    pub fn get_latest_contribution(&self) -> Result<&Contribution, CoordinatorError> {
        match self.contributions.last() {
            Some(contribution) => Ok(contribution),
            _ => Err(CoordinatorError::NoContributions),
        }
    }

    ///
    /// Attempts to add a new contribution from a contributor to the chunk.
    /// Upon success, releases the lock on this chunk to allow a verifier to
    /// check the contribution for correctness.
    ///
    /// If the operations succeed, returns `Ok(())`. Otherwise, returns `CoordinatorError`.
    ///
    #[inline]
    pub fn add_contribution(
        &mut self,
        chunk_id: u64,
        contributor_id: String,
        contributor_base_url: &str,
    ) -> Result<(), CoordinatorError> {
        // Construct the starting contribution template for this chunk.
        let contribution_id = self.contribution_id();
        let contribution =
            Contribution::new_contributor(chunk_id, contribution_id, contributor_id, contributor_base_url)?;

        // Add the contribution to this chunk.
        self.contributions.push(contribution);
        // Releases the lock on this chunk.
        self.lock_holder = None;

        Ok(())
    }

    /// Returns `true` if the participant does not already hold the lock.
    /// Otherwise, returns `false`.
    #[inline]
    pub(crate) fn acquire_lock(&mut self, participant_id: &str) -> Result<(), CoordinatorError> {
        // Check that the participant has not already contributed before.
        let matches: Vec<_> = self
            .contributions
            .par_iter()
            .filter(|contribution| *contribution.get_contributor_id() == Some(participant_id.to_string()))
            .collect();
        if !matches.is_empty() {
            return Err(CoordinatorError::AlreadyContributed);
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
        let contribution = self.get_latest_contribution()?;
        if self.is_locked_by(participant_id)
            && *contribution.get_contributor_id() == Some(participant_id.to_string())
            && contribution.get_contributed_location().is_none()
            && !contribution.is_verified()
        {
            return Ok(());
        }

        self.lock_holder = Some(participant_id.to_string());
        Ok(())

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

#[cfg(test)]
mod tests {
    use crate::testing::prelude::*;

    // #[test]
    // fn test_update_chunk() {
    //     let mut expected = test_round_1_json().unwrap().chunks[0].clone();
    //     expected.acquire_lock("test_updated_contributor");
    //
    //     let candidate = test_round_1().unwrap().get_chunk_mut(0).unwrap();
    //     assert!(candidate.update_chunk(0, &expected));
    //     assert_eq!(expected, *candidate);
    // }
}
