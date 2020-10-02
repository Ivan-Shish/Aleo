#[derive(Debug, Error)]
pub enum VerifierError {
    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("Request {} sent to {} errored", _0, _1)]
    FailedRequest(String, String),
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
