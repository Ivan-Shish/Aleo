use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignedContributedData {
    pub data: Value,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignedVerifiedData {
    pub data: Value,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContributedData {
    pub challenge_hash: String,
    pub response_hash: String,
    pub contribution_duration: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedData {
    pub challenge_hash: String,
    pub response_hash: String,
    pub new_challenge_hash: String,
    pub verification_duration: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContributionUploadUrl {
    pub chunk_id: String,
    pub participant_id: String,
    pub write_url: String,
}
