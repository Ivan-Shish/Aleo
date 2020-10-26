use crate::coordinator::CoordinatorError;

use serde::{Deserialize, Serialize};
use serde_diff::SerdeDiff;
use tracing::debug;

///
/// The contribution data for a given chunkID that is signed by the participant.
///
/// This data is comprised of:
/// 1. The hash of the challenge file.
/// 2. The hash of the response file.
/// 3. The hash of the new challenge file if the participant was a verifier.
/// 3. The duration the contributor (in milliseconds) took to generate the response.
///
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct ContributionData {
    /// The hash of the challenge file.
    challenge_hash: String,

    /// The hash of the response file.
    response_hash: String,

    /// The hash of the new challenge file.
    new_challenge_hash: Option<String>,

    /// The duration the contributor took for a given chunk (in milliseconds).
    contribution_duration: i64,
}

///
/// The data for a given contribution and the signature to the data created by the
/// contributor.
///
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
pub struct ContributionSignature {
    /// The signature of the contribution hash data.
    signature: String,

    /// The contribution data that is being signed.
    data: ContributionData,
}

impl ContributionSignature {
    /// Creates a new instance of `ContributionSignature`.
    #[inline]
    pub(crate) fn new(
        signature: String,
        challenge_hash: String,
        response_hash: String,
        new_challenge_hash: Option<String>,
        contribution_duration: i64,
    ) -> Result<Self, CoordinatorError> {
        debug!("Starting to create contribution signature");

        let data = ContributionData {
            challenge_hash,
            response_hash,
            new_challenge_hash,
            contribution_duration,
        };

        debug!("Completed creating contribution signature");

        Ok(Self { signature, data })
    }

    /// Returns a reference to the signature.
    #[inline]
    pub fn get_signature(&self) -> &str {
        &self.signature
    }

    /// Returns a reference to the challenge hash.
    #[inline]
    pub fn get_challenge_hash(&self) -> &str {
        &self.data.challenge_hash
    }

    /// Returns a reference to the response hash.
    #[allow(dead_code)]
    #[inline]
    pub fn get_response_hash(&self) -> &str {
        &self.data.response_hash
    }

    /// Returns a reference to the new challenge hash, if it exists.
    /// Otherwise returns `None`.
    #[allow(dead_code)]
    #[inline]
    pub fn get_new_challenge_hash(&self) -> &Option<String> {
        &self.data.new_challenge_hash
    }

    /// Returns a reference to the contribution duration.
    #[allow(dead_code)]
    #[inline]
    pub fn get_contribution_duration(&self) -> i64 {
        self.data.contribution_duration
    }
}
