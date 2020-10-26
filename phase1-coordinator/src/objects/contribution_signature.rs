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
///
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct ContributionData {
    /// The hash of the challenge file.
    challenge_hash: String,

    /// The hash of the response file.
    response_hash: String,

    /// The hash of the next challenge file.
    next_challenge_hash: Option<String>,
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
        challenge_hash: Vec<u8>,
        response_hash: Vec<u8>,
        next_challenge_hash: Option<Vec<u8>>,
    ) -> Result<Self, CoordinatorError> {
        debug!("Starting to create contribution signature");

        // Check that the signature is 64 bytes.
        if hex::decode(&signature)?.len() != 64 {
            return Err(CoordinatorError::ContributionSignatureSizeMismatch);
        }

        // Check that the challenge hash is 64 bytes.
        if challenge_hash.len() != 64 {
            return Err(CoordinatorError::ChallengeHashSizeInvalid);
        }

        // Check that the response hash is 64 bytes.
        if response_hash.len() != 64 {
            return Err(CoordinatorError::ResponseHashSizeInvalid);
        }

        // Check that the next challenge hash is 64 bytes.
        if let Some(next_challenge_hash) = &next_challenge_hash {
            if next_challenge_hash.len() != 64 {
                return Err(CoordinatorError::NextChallengeHashSizeInvalid);
            }
        }

        let challenge_hash = hex::encode(challenge_hash);
        let response_hash = hex::encode(response_hash);
        let next_challenge_hash = next_challenge_hash.map(|h| hex::encode(h));

        let data = ContributionData {
            challenge_hash,
            response_hash,
            next_challenge_hash,
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
        &self.data.next_challenge_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::prelude::*;

    use itertools::assert_equal;
    use setup_utils::calculate_hash;

    #[test]
    pub fn test_contribution_signature() {
        // TODO (raychu86): Use real challenge, response, and next_challenge files.

        let dummy_challenge = vec![2; 128];
        let dummy_response = vec![3; 128];
        let dummy_next_challenge = vec![4; 128];

        let challenge_hash = calculate_hash(&dummy_challenge);
        let response_hash = calculate_hash(&dummy_response);
        let next_challenge_hash = calculate_hash(&dummy_next_challenge);

        let signature = vec![2u8; 64];
        let signature_string = hex::encode(signature);

        let contribution_signature = ContributionSignature::new(
            signature_string.clone(),
            challenge_hash.to_vec(),
            response_hash.to_vec(),
            Some(next_challenge_hash.to_vec()),
        )
        .unwrap();

        let contribution_signature_2 =
            ContributionSignature::new(signature_string, challenge_hash.to_vec(), response_hash.to_vec(), None)
                .unwrap();

        let verifier_contribution_signature = serde_json::to_vec_pretty(&contribution_signature).unwrap();
        let contributor_contribution_signature = serde_json::to_vec_pretty(&contribution_signature_2).unwrap();

        println!("contribution_signature {:#?}", contribution_signature);
        println!("contribution_signature_2 {:#?}", contribution_signature_2);

        println!(
            "verifier_contribution_signature len: {:?}",
            verifier_contribution_signature.len()
        );
        println!(
            "contributor_contribution_signature len: {:?}",
            contributor_contribution_signature.len()
        );

        assert_eq!(0, 1);
    }

    // TODO (raychu86): Implment tests for contribution signature.
}
