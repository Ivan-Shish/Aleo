use serde::{Deserialize, Serialize};

///
/// This lock response bundles the data required for the verifier
/// to perform a valid verification.
///
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LockResponse {
    /// The chunk id
    #[serde(alias = "chunkId")]
    pub chunk_id: u64,

    /// The contribution id
    #[serde(alias = "contributionId")]
    pub contribution_id: u64,

    /// Indicator if the chunk was locked
    pub locked: bool,

    /// The participant id related to the lock
    #[serde(alias = "participantID")]
    pub participant_id: String,

    #[serde(alias = "challengeLocator")]
    pub challenge_locator: String,

    #[serde(alias = "challengeChunkId")]
    pub challenge_chunk_id: u64,

    #[serde(alias = "challengeContributionId")]
    pub challenge_contribution_id: u64,

    #[serde(alias = "responseLocator")]
    pub response_locator: String,

    #[serde(alias = "nextChallengeLocator")]
    pub next_challenge_locator: String,

    #[serde(alias = "nextChallengeChunkId")]
    pub next_challenge_chunk_id: u64,

    #[serde(alias = "nextChallengeContributionId")]
    pub next_challenge_contribution_id: u64,
}
