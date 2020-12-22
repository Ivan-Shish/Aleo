///
/// This lock response bundles the data required for the verifier
/// to perform a valid verification.
///
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LockResponse {
    /// The chunk id
    #[serde(alias = "chunkId")]
    pub chunk_id: u64,

    /// Indicator if the chunk was locked
    pub locked: bool,

    /// The participant id related to the lock
    #[serde(alias = "participantID")]
    pub participant_id: String,

    #[serde(alias = "challengeLocator")]
    pub challenge_locator: String,

    #[serde(alias = "responseLocator")]
    pub response_locator: String,

    #[serde(alias = "nextChallengeLocator")]
    pub next_challenge_locator: String,
}
