use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContributeError {
    #[error("Could not read passphrase")]
    CouldNotReadPassphraseError,
    #[error("Lane {0} did not contain chunk with ID: {1}")]
    LaneDidNotContainChunkWithIDError(String, String),
    #[error("Lane {0} already contains chunk with ID: {1}")]
    LaneAlreadyContainsChunkWithIDError(String, String),
    #[error("Failed running contribute")]
    FailedRunningContributeError,
    #[error("Lane was null: {0}")]
    LaneWasNullError(String),
    #[error("Unsupported decryptor")]
    UnsupportedDecryptorError,
}

#[derive(Debug, Error)]
pub enum GenerateError {}

#[cfg(feature = "azure")]
#[derive(Debug, Error)]
pub enum HttpError {
    #[error("Could not upload to azure, status was: {0}")]
    CouldNotUploadToAzureError(String),
    #[error("Could not parse SAS: {0}")]
    CouldNotParseSAS(String),
}

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("Unknown upload mode: {0}")]
    UnknownUploadModeError(String),
}

#[derive(Debug, Error)]
pub enum CLIError {
    #[error("{}", _0)]
    ContributeError(ContributeError),

    #[error("{}", _0)]
    GenerateError(GenerateError),
}

impl From<ContributeError> for CLIError {
    fn from(error: ContributeError) -> Self {
        CLIError::ContributeError(error)
    }
}

impl From<GenerateError> for CLIError {
    fn from(error: GenerateError) -> Self {
        CLIError::GenerateError(error)
    }
}
