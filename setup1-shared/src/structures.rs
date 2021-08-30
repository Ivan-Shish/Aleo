//! The data structures shared between coordinator, contributor and verifier

use serde::{Deserialize, Serialize};

/// The kind of a setup
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SetupKind {
    Development,
    Inner,
    Outer,
    Universal,
}

impl SetupKind {
    pub fn as_string(&self) -> String {
        match self {
            SetupKind::Development => "development".to_owned(),
            SetupKind::Inner => "inner".to_owned(),
            SetupKind::Outer => "outer".to_owned(),
            SetupKind::Universal => "universal".to_owned(),
        }
    }
}

/// The public settings of a setup to let the contributors know
/// what kind of a setup is running at the moment and some
/// other details
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicSettings {
    pub setup: SetupKind,
    pub check_reliability: bool,
}

impl PublicSettings {
    /// Encodes self as a JSON message to a vector of bytes
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decodes a JSON message from a slice of bytes into Self
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// The combination of request token and PIN, used to tweet
/// on a contributor's behalf by the coordinator.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg(feature = "twitter")]
pub struct TwitterInfo {
    pub request_token: egg_mode::KeyPair,
    pub pin: String,
}

/// The status of the contributor related to the current round
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ContributorStatus {
    Queue,
    Round,
    Finished,
    Other,
}
