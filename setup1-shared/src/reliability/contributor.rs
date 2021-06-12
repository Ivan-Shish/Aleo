use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

/// Message from contributor to coordinator
#[derive(Debug, Deserialize, Serialize)]
pub enum ContributorMessage {
    BandwidthChallenge(Vec<u8>),
    CpuChallenge(Vec<u8>),
    Error(String),
    Pong { id: i64 },
}

impl ContributorMessage {
    /// Encodes self as a JSON message to a vector of bytes
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decodes a JSON message from a slice of bytes into Self
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

impl Display for ContributorMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ContributorMessage::*;
        let text = match self {
            BandwidthChallenge(_) => "BandwidthChallenge(Vec<u8>)".to_owned(),
            CpuChallenge(_) => "CpuChallenge(Vec<u8>)".to_owned(),
            Error(message) => format!("Error({})", message),
            Pong { id } => format!("Pong {{ id: {} }}", id),
        };
        write!(f, "{}", text)
    }
}
