use phase1_coordinator::CoordinatorError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VerifierError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("Coordinator Error {}", _0)]
    CoordinatorError(CoordinatorError),

    #[error("Failed to download a challenge at {}", _0)]
    FailedChallengeDownload(String),

    #[error("Request {} sent to {} errored", _0, _1)]
    FailedRequest(String, String),

    #[error("Failed to download a response at {}", _0)]
    FailedResponseDownload(String),

    #[error("Failed to upload a new challenge file to {}", _0)]
    FailedChallengeUpload(String),

    #[error("Mismatched response hashes")]
    MismatchedResponseHashes,

    #[error("Next challenge file missing stored response hash")]
    MissingStoredResponseHash,
}

impl From<anyhow::Error> for VerifierError {
    fn from(error: anyhow::Error) -> Self {
        VerifierError::Crate("anyhow", format!("{:?}", error))
    }
}

impl From<CoordinatorError> for VerifierError {
    fn from(error: CoordinatorError) -> Self {
        VerifierError::CoordinatorError(error)
    }
}

impl From<hex::FromHexError> for VerifierError {
    fn from(error: hex::FromHexError) -> Self {
        VerifierError::Crate("hex", format!("{:?}", error))
    }
}

impl From<reqwest::Error> for VerifierError {
    fn from(error: reqwest::Error) -> Self {
        VerifierError::Crate("reqwest", format!("{:?}", error))
    }
}

impl From<std::io::Error> for VerifierError {
    fn from(error: std::io::Error) -> Self {
        VerifierError::Crate("std::io", format!("{:?}", error))
    }
}

impl From<serde_json::Error> for VerifierError {
    fn from(error: serde_json::Error) -> Self {
        VerifierError::Crate("serde_json", format!("{:?}", error))
    }
}

impl From<snarkvm_dpc::AccountError> for VerifierError {
    fn from(error: snarkvm_dpc::AccountError) -> Self {
        VerifierError::Crate("snarkvm_dpc", format!("{:?}", error))
    }
}
