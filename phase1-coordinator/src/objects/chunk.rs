use crate::{
    objects::{Contribution, Participant},
    CoordinatorError,
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use serde_diff::SerdeDiff;
use tracing::trace;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    chunk_id: u64,
    lock_holder: Option<Participant>,
    #[serde_diff(opaque)]
    contributions: Vec<Contribution>,
}

impl Chunk {
    ///
    /// Creates a new instance of `Chunk`.
    ///
    /// Checks that the given participant is a verifier,
    /// as this function is intended for use to initialize
    /// a new round by the coordinator.
    ///
    /// This function creates one contribution with a
    /// contribution ID of `0`.
    ///
    #[inline]
    pub fn new(chunk_id: u64, participant: Participant, verifier_base_url: &str) -> Result<Self, CoordinatorError> {
        match participant.is_verifier() {
            // Construct the starting contribution template for this chunk.
            true => Ok(Self {
                chunk_id,
                lock_holder: None,
                contributions: vec![Contribution::new_verifier(chunk_id, 0, participant, verifier_base_url)?],
            }),
            false => Err(CoordinatorError::ExpectedVerifier),
        }
    }

    /// Returns the assigned chunk ID.
    #[inline]
    pub fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    /// Returns `true` if the current chunk is locked. Otherwise, returns `false`.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.lock_holder.is_some()
    }

    /// Returns `true` if the current chunk is unlocked. Otherwise, returns `false`.
    #[inline]
    pub fn is_unlocked(&self) -> bool {
        !self.is_locked()
    }

    /// Returns `true` if the current chunk is locked by the given participant.
    /// Otherwise, returns `false`.
    #[inline]
    pub fn is_locked_by(&self, participant: &Participant) -> bool {
        // Retrieve the current lock holder, or return `false` if the chunk is unlocked.
        match &self.lock_holder {
            Some(lock_holder) => *lock_holder == *participant,
            None => false,
        }
    }

    /// Returns the current number of contributions in this chunk,
    /// irrespective of the state of each contribution.
    #[inline]
    pub fn current_contribution_id(&self) -> u64 {
        (self.contributions.len() - 1) as u64
    }

    /// Returns a reference to the current contribution in this chunk,
    /// irrespective of the state of the contribution.
    #[inline]
    pub fn current_contribution(&self) -> Result<&Contribution, CoordinatorError> {
        self.get_contribution(self.current_contribution_id())
    }

    /// Returns a reference to a contribution given a contribution ID.
    #[inline]
    pub fn get_contribution(&self, contribution_id: u64) -> Result<&Contribution, CoordinatorError> {
        match self.contributions.get(contribution_id as usize) {
            Some(contribution) => Ok(contribution),
            _ => Err(CoordinatorError::ContributionMissing),
        }
    }

    /// Returns a mutable reference to a contribution given a contribution ID.
    #[inline]
    pub fn get_contribution_mut(&mut self, contribution_id: u64) -> Result<&mut Contribution, CoordinatorError> {
        match self.contributions.get_mut(contribution_id as usize) {
            Some(contribution) => Ok(contribution),
            _ => Err(CoordinatorError::ContributionMissing),
        }
    }

    /// Returns a reference to a list of contributions in this chunk.
    #[inline]
    pub fn get_contributions(&self) -> &Vec<Contribution> {
        &self.contributions
    }

    ///
    /// Attempts to acquire the lock for the given participant.
    ///
    /// If the chunk is locked already, returns a `CoordinatorError`.
    ///
    /// If the participant is a contributor, check that they have not
    /// contributed to this chunk before and that the current contribution
    /// is already verified.
    ///
    /// If the participant is a verifier, check that the current contribution
    /// has not been verified yet.
    ///
    #[inline]
    pub(crate) fn acquire_lock(&mut self, participant: Participant) -> Result<(), CoordinatorError> {
        // Check that this chunk is not locked before attempting to acquire the lock.
        if self.is_locked() {
            return Err(CoordinatorError::ChunkLockAlreadyAcquired);
        }

        // If the participant is a contributor, check that they have not already contributed to this chunk before.
        if let Participant::Contributor(contributor_id) = &participant {
            // Fetch all contributions with this contributor ID.
            let matches: Vec<_> = self
                .contributions
                .par_iter()
                .filter(|contribution| *contribution.get_contributor() == Some(participant.clone()))
                .collect();
            if !matches.is_empty() {
                return Err(CoordinatorError::ContributorAlreadyContributed);
            }

            // If the lock is currently held by this participant,
            // the current contributor ID is this contributor,
            // the current contributed location is empty,
            // and the current contribution is not verified,
            // then it could mean this contributor lost their
            // connection and is attempting to reconnect.
            //
            // In this case, no further action needs to be taken,
            // and we may return true.
            let contribution = self.current_contribution()?;
            if self.is_locked_by(&participant)
                && *contribution.get_contributor() == Some(participant.clone())
                && contribution.get_contributed_location().is_none()
                && !contribution.is_verified()
            {
                return Ok(());
            }

            // Check that the current contribution in this chunk has been verified.
            if !self.current_contribution()?.is_verified() {
                return Err(CoordinatorError::ChunkMissingVerification);
            }
        }

        // If the participant is a verifier, check that they have not already contributed to this chunk before.
        if participant.is_verifier() {
            // Check that the current contribution in this chunk has NOT been verified.
            if self.current_contribution()?.is_verified() {
                return Err(CoordinatorError::ChunkAlreadyVerified);
            }
        }

        // Set the lock holder as the participant.
        self.lock_holder = Some(participant);
        Ok(())
    }

    ///
    /// Attempts to add a new contribution from a contributor to this chunk.
    /// Upon success, releases the lock on this chunk to allow a verifier to
    /// check the contribution for correctness.
    ///
    /// This function is intended to be called by an authorized contributor
    /// holding a lock on the chunk.
    ///
    /// If the operations succeed, returns `Ok(())`. Otherwise, returns `CoordinatorError`.
    ///
    #[inline]
    pub fn add_contribution(
        &mut self,
        participant: Participant,
        contributor_base_url: &str,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Check that this chunk is locked by the contributor before attempting to add the contribution.
        if !self.is_locked_by(&participant) {
            return Err(CoordinatorError::ChunkNotLockedOrWrongParticipant);
        }

        // Construct the starting contribution template for this chunk.
        let contribution_id = self.current_contribution_id();
        let contribution =
            Contribution::new_contributor(self.chunk_id(), contribution_id, participant, contributor_base_url)?;

        // Add the contribution to this chunk.
        self.contributions.push(contribution);
        // Releases the lock on this chunk.
        self.lock_holder = None;

        Ok(())
    }

    ///
    /// Updates the contribution corresponding to the given contribution ID as verified.
    ///
    /// This function is intended to be called by an authorized verifier
    /// holding a lock on the chunk.
    ///
    /// The underlying function checks that the contribution has a verifier assigned to it.
    ///
    #[inline]
    pub fn verify_contribution(
        &mut self,
        contribution_id: u64,
        participant: &Participant,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that this chunk is locked by the verifier before attempting to verify contribution.
        if !self.is_locked_by(participant) {
            return Err(CoordinatorError::ChunkNotLockedOrWrongParticipant);
        }

        // Fetch the contribution to be verified from the chunk.
        let mut contribution = self.get_contribution_mut(contribution_id)?;

        // Attempt to verify the contribution.
        match contribution.is_verified() {
            // Case 1 - Check that the contribution is not verified yet.
            true => Err(CoordinatorError::ContributionAlreadyVerified),
            // Case 2 - If the contribution is not verified, attempt to set it to verified.
            false => {
                contribution.try_verify(participant)?;
                trace!("Verification of contribution {} succeeded", contribution_id);
                Ok(())
            }
        }
    }

    /// Generates an unique number representing the current state of the chunk.
    #[inline]
    pub fn version(&self) -> u32 {
        let matching_contributions: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.get_contributor().is_some() as u32)
            .sum();
        let matching_verifications: u32 = self
            .contributions
            .par_iter()
            .map(|contribution| contribution.get_verifier().is_some() as u32)
            .sum();
        return matching_contributions + matching_verifications;
    }

    ///
    /// Returns `true` if contributions have been added by all contributors
    /// to this chunk and all contributions have been verified.
    /// Otherwise, returns `false`.
    ///
    #[inline]
    pub fn is_complete(&self, num_contributors: u64) -> bool {
        let missing_contributions = (self.current_contribution_id() + 1) < num_contributors;
        let extraneous_contributions = (self.current_contribution_id() + 1) > num_contributors;
        let missing_verifications = !self
            .get_contributions()
            .par_iter()
            .filter(|contribution| !contribution.is_verified())
            .collect::<Vec<_>>()
            .is_empty();

        !(missing_contributions || extraneous_contributions || missing_verifications)
    }
}

#[cfg(test)]
mod tests {
    // use crate::testing::prelude::*;

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
