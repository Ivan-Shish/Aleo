use crate::{errors::VerifierError, utils::authenticate};
use phase1_coordinator::Participant;
use snarkos_toolkit::account::{Address, ViewKey};

use reqwest::Client;
use std::str::FromStr;
use tracing::error;

/// Request to the verifier to run a verification operation on the contribution
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VerifierRequest {
    pub method: String,
    pub chunk_id: u64,
}

///
/// The verifier used to manage and dispatch/execute verifier operations
/// to the remote coordinator.
///
#[derive(Debug, Clone)]
pub struct Verifier {
    /// The url of the coordinator that will be
    pub(crate) coordinator_api_url: String,

    /// The view key that will be used for server authentication
    pub(crate) view_key: String,

    /// The identity of the verifier
    pub(crate) verifier: Participant,
}

impl Verifier {
    /// Initialize a new verifier
    pub fn new(coordinator_api_url: String, view_key: String) -> Result<Self, VerifierError> {
        let verifier_id = Address::from_view_key(&ViewKey::from_str(&view_key)?)?.to_string();

        Ok(Self {
            coordinator_api_url,
            view_key,
            verifier: Participant::Verifier(verifier_id),
        })
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID for a given verifier.
    ///
    /// On success, this function returns the `LockResponse`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn lock_chunk(&self, chunk_id: u64) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/coordinator/verify/chunks/{}/lock", chunk_id);

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => Ok(response.text().await?),
            Err(_) => {
                error!("Verifier failed to lock chunk");
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to run verification in the current round for a given chunk ID
    ///
    /// On success, this function copies the current contribution into the next transcript locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn verify_contribution(&self, chunk_id: u64) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/coordinator/verify/chunks/{}", chunk_id);

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => Ok(response.text().await?),
            Err(_) => {
                error!("Verifier failed to verify contribution");
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }
}
