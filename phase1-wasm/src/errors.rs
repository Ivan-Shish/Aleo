use thiserror::Error;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Clone, Copy, Debug, Error)]
pub enum ContributeError {
    #[error("Challenge hash size is invalid")]
    ChallengeHashSizeInvalid,
    #[error("Response hash size is invalid")]
    ResponseHashSizeInvalid,
    #[error("Next challenge hash size is invalid")]
    NextChallengeHashSizeInvalid,
    #[error("Contribution signature size does not match")]
    ContributionSignatureSizeMismatch,
}
