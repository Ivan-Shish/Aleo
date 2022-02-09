use crate::errors::ContributeError;
use serde::{Deserialize, Serialize};
use serde_diff::SerdeDiff;

use wasm_bindgen::prelude::*;

///
/// The contribution state for a given chunk ID that is signed by the participant.
///
/// This state is comprised of:
/// 1. The hash of the challenge file.
/// 2. The hash of the response file.
/// 3. The hash of the next challenge file if the participant was a verifier.
///
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
#[serde(rename_all = "camelCase")]
pub struct ContributionState {
    /// The hash of the challenge file.
    challenge_hash: String,
    /// The hash of the response file.
    response_hash: String,
    /// The hash of the next challenge file.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_challenge_hash: Option<String>,
}

impl ContributionState {
    /// Creates a new instance of `ContributionFileSignature`.
    pub fn new(
        challenge_hash: Vec<u8>,
        response_hash: Vec<u8>,
        next_challenge_hash: Option<Vec<u8>>,
    ) -> Result<Self, ContributeError> {
        // Check that the challenge hash is 64 bytes.
        if challenge_hash.len() != 64 {
            return Err(ContributeError::ChallengeHashSizeInvalid);
        }

        // Check that the response hash is 64 bytes.
        if response_hash.len() != 64 {
            return Err(ContributeError::ResponseHashSizeInvalid);
        }

        // Check that the next challenge hash is 64 bytes, if it exists.
        if let Some(next_challenge_hash) = &next_challenge_hash {
            if next_challenge_hash.len() != 64 {
                return Err(ContributeError::NextChallengeHashSizeInvalid);
            }
        }

        Ok(ContributionState {
            challenge_hash: hex::encode(challenge_hash),
            response_hash: hex::encode(response_hash),
            next_challenge_hash: next_challenge_hash.map(|h| hex::encode(h)),
        })
    }

    /// Returns the message that should be signed for the `ContributionFileSignature`.
    pub fn signature_message(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}

///
/// The signature and state of the contribution.
///
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
pub struct ContributionFileSignature {
    /// The signature of the contribution state.
    signature: String,
    /// The state of the contribution that is signed.
    state: ContributionState,
}

impl ContributionFileSignature {
    /// Creates a new instance of `ContributionFileSignature`.
    pub fn new(signature: String, state: ContributionState) -> anyhow::Result<Self> {
        tracing::debug!("Starting to create contribution signature");
        // Check that the signature is 64 bytes.
        if hex::decode(&signature)?.len() != 64 {
            return Err(ContributeError::ContributionSignatureSizeMismatch.into());
        }
        tracing::debug!("Completed creating contribution signature");
        Ok(Self { signature, state })
    }

    /// Returns a reference to the signature.
    pub fn get_signature(&self) -> &str {
        &self.signature
    }
}
