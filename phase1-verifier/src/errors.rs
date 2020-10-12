#[derive(Debug, Error)]
pub enum VerifierError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("Failed to download a challenge at {}", _0)]
    FailedChallengeDownload(String),

    #[error("Failed to lock a chunk")]
    FailedLock,

    #[error("Request {} sent to {} errored", _0, _1)]
    FailedRequest(String, String),

    #[error("Failed to download a response at {}", _0)]
    FailedResponseDownload(String),

    #[error("Failed to upload a new challenge file to {}", _0)]
    FailedChallengeUpload(String),

    #[error("The coordinator failed to verify the uploaded challenge file {}", _0)]
    FailedVerification(String),
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

impl From<snarkos_toolkit::errors::AddressError> for VerifierError {
    fn from(error: snarkos_toolkit::errors::AddressError) -> Self {
        VerifierError::Crate("snarkos", format!("{:?}", error))
    }
}

impl From<snarkos_toolkit::errors::ViewKeyError> for VerifierError {
    fn from(error: snarkos_toolkit::errors::ViewKeyError) -> Self {
        VerifierError::Crate("snarkos", format!("{:?}", error))
    }
}
