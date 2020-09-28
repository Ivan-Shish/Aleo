use crate::{
    objects::{participant::*, Participant},
    CoordinatorError,
};

use serde::{Deserialize, Serialize};
use tracing::trace;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Contribution {
    #[serde(deserialize_with = "deserialize_optional_contributor_from_string")]
    contributor_id: Option<Participant>,
    #[serde(rename = "contributedLocation")]
    contributed_locator: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_verifier_from_string")]
    verifier_id: Option<Participant>,
    #[serde(rename = "verifiedLocation")]
    verified_locator: Option<String>,
    verified: bool,
}

impl Contribution {
    /// TODO (howardwu): Extract URL out to environment.
    ///
    /// Creates a new contributor instance of `Contribution`.
    ///
    /// This function is called when adding a new contribution
    /// to an *existing* chunk only. It should be assumed that
    /// the chunk is in the process of being locked by a new
    /// contributor when calling this function.
    ///
    #[inline]
    pub(crate) fn new_contributor(
        participant: Participant,
        contributed_locator: String,
    ) -> Result<Self, CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        Ok(Self {
            contributor_id: Some(participant),
            contributed_locator: Some(contributed_locator),
            verifier_id: None,
            verified_locator: None,
            verified: false,
        })
    }

    /// TODO (howardwu): Extract URL out to environment.
    ///
    /// Creates a new verifier instance of `Contribution`.
    ///
    /// This function is called when adding a new contribution
    /// to a *new* chunk only. It should be assumed that
    /// the chunk is new and in the process of being created
    /// by the coordinator when calling this function.
    ///
    #[inline]
    pub(crate) fn new_verifier(
        contribution_id: u64,
        participant: Participant,
        verifier_locator: String,
    ) -> Result<Self, CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that this function is only used to
        // initialize a new round in the ceremony.
        if contribution_id != 0 {
            return Err(CoordinatorError::ContributionIdIsNonzero);
        }

        // Create a new contribution instance.
        // As this is function is only used for initialization,
        // we can safely set `verified` to `true`.
        let mut contribution = Self {
            contributor_id: None,
            contributed_locator: None,
            verifier_id: Some(participant),
            verified_locator: Some(verifier_locator),
            verified: true,
        };

        Ok(contribution)
    }

    /// TODO (howardwu): Extract URL out to environment.
    ///
    /// Assign a verifier to this instance of `Contribution`.
    ///
    /// If this contribution already has a verifier or verified locator,
    /// returns a `CoordinatorError`.
    ///
    /// Note that this function does NOT set the state of `verified`.
    /// This approach allows a verifier to assign themselves to verify
    /// this contribution, and only after completing verification,
    /// proceed to call another function to update `verified`.
    ///
    #[inline]
    pub(crate) fn assign_verifier(
        &mut self,
        participant: Participant,
        verifier_locator: String,
    ) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that this contribution does not have a verifier.
        if self.verifier_id.is_some() {
            return Err(CoordinatorError::ContributionAlreadyAssignedVerifier);
        }

        // Check that this contribution does not have a verified locator.
        if self.verified_locator.is_some() {
            return Err(CoordinatorError::ContributionAlreadyAssignedVerifiedLocator);
        }

        // Check that this contribution is not verified.
        if self.verified {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        self.verifier_id = Some(participant);
        self.verified_locator = Some(verifier_locator);
        Ok(())
    }

    /// TODO (howardwu): Check that verified locator is stored.
    ///
    /// Updates `verified` to `true` in this instance of `Contribution`,
    /// if the verifier ID and verified location are valid.
    ///
    /// If this contribution has already been verified,
    /// returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn try_verify(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that this contribution has a verifier.
        let verifier_id = match &self.verifier_id {
            Some(verifier_id) => verifier_id,
            None => return Err(CoordinatorError::ContributionMissingVerifier),
        };

        // Check that the verifier matches the given participant.
        if *verifier_id != *participant {
            return Err(CoordinatorError::UnauthorizedChunkVerifier);
        }

        // Check that this contribution has a verified locator.
        if self.verified_locator.is_none() {
            return Err(CoordinatorError::ContributionMissingVerifiedLocator);
        }

        // Check that this contribution has not been verified.
        if self.verified {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        trace!("Setting contribution to verified");
        self.verified = true;
        Ok(())
    }

    /// Returns `true` if the contribution has been verified.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_verified(&self) -> bool {
        self.verified
    }

    /// Returns a reference to the contributor, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_contributor(&self) -> &Option<Participant> {
        &self.contributor_id
    }

    /// Returns a reference to the contributor location, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_contributed_location(&self) -> &Option<String> {
        &self.contributed_locator
    }

    /// Returns a reference to the verifier, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_verifier(&self) -> &Option<Participant> {
        &self.verifier_id
    }

    /// Returns a reference to the verifier location, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_verified_location(&self) -> &Option<String> {
        &self.verified_locator
    }
}
