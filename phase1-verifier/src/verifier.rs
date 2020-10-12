use crate::{errors::VerifierError, utils::authenticate};
use phase1::helpers::CurveKind;
use phase1_cli::transform_pok_and_correctness;
use phase1_coordinator::{environment::Environment, phase1_chunked_parameters, Participant};
use snarkos_toolkit::account::{Address, ViewKey};
use zexe_algebra::{Bls12_377, BW6_761};

use reqwest::Client;
use std::{str::FromStr, thread::sleep, time::Duration};
use tracing::{error, info, trace};

///
/// This lock response bundles the data required for the contributor/verifier
/// to perform a valid contribution or verification.
///
#[derive(Debug, Serialize, Deserialize)]
pub struct LockResponse {
    /// The chunk id
    #[serde(alias = "chunkId")]
    pub chunk_id: u64,

    /// Indicator if the chunk was locked
    pub locked: bool,

    /// The participant id related to the lock
    #[serde(rename(deserialize = "participantID"))]
    #[serde(alias = "participantID")]
    pub participant_id: String,

    /// The locator of the challenge file that the participant will download
    #[serde(alias = "challengeLocator")]
    pub next_challenge_locator: String,

    /// The locator where the participant will upload their completed contribution/verification.
    #[serde(alias = "responseLocator")]
    pub response_locator: String,
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

    /// The coordinator environment
    pub(crate) environment: Environment,
}

impl Verifier {
    /// Initialize a new verifier
    pub fn new(coordinator_api_url: String, view_key: String, environment: Environment) -> Result<Self, VerifierError> {
        let verifier_id = Address::from_view_key(&ViewKey::from_str(&view_key)?)?.to_string();

        Ok(Self {
            coordinator_api_url,
            view_key,
            verifier: Participant::Verifier(verifier_id),
            environment,
        })
    }

    ///
    /// Attempts to acquire the lock of a given chunk ID for a given verifier.
    ///
    /// On success, this function returns the `LockResponse`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn lock_chunk(&self) -> Result<LockResponse, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = "/coordinator/verifier/lock";

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;

        info!("Verifier attempting to lock a chunk");

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                // Parse the lock response
                let json_response = response.json().await?;
                let lock_response = serde_json::from_value::<LockResponse>(json_response)?;

                Ok(lock_response)
            }
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
    /// Attempts to run verification in the current round for a given verified locator
    ///
    /// On success, this function copies the current contribution into the next transcript locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn verify_contribution(&self, verified_locator: &str) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/coordinator/verify/{}", verified_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!(
            "Verifier running verification of a response file at {} ",
            verified_locator
        );

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

    ///
    /// Attempts to download the unverified response file from the coordinator
    ///
    /// On success, this function returns the full response file buffer.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn download_response_file(&self, response_locator: &str) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/coordinator/locator/{}", response_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!("Verifier downloading a response file at {} ", response_locator);

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => Ok(response.bytes().await?.to_vec()),
            Err(_) => {
                error!("Verifier failed to download the response file: {}", response_locator);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to download the  response file from the coordinator
    ///
    /// On success, this function returns the full challenge file buffer.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn download_challenge_file(&self, challenge_locator: &str) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/coordinator/locator/{}", challenge_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        info!("Verifier downloading a challenge file at {} ", challenge_locator);

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => Ok(response.bytes().await?.to_vec()),
            Err(_) => {
                error!("Verifier failed to download the challenge file: {}", challenge_locator);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to upload the next challenge locator file to the coordinator
    ///
    /// On success, this function returns the full challenge file buffer.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub async fn upload_next_challenge_locator_file(
        &self,
        next_challenge_locator: &str,
        next_challenge_file_bytes: Vec<u8>,
    ) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/coordinator/locator/{}", next_challenge_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("/api{}", path);
        let authentication = authenticate(&view_key, &method, &signature_path)?;

        info!(
            "Verifier uploading a response with size {} to {} ",
            next_challenge_file_bytes.len(),
            next_challenge_locator
        );

        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .body(next_challenge_file_bytes)
            .send()
            .await
        {
            Ok(response) => Ok(response.text().await?),
            Err(_) => {
                error!(
                    "Verifier failed to upload the next challenge file: {}",
                    next_challenge_locator
                );
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Start the verifier loop. Polls the coordinator to lock and verify chunks.
    ///
    pub async fn start_verifier(&self) {
        // Run the verifier loop
        loop {
            // Attempt to lock a chunk
            if let Ok(lock_response) = self.lock_chunk().await {
                info!("Verifier locked a chunk {} ", lock_response.chunk_id);

                // Download the response file from the coordinator
                let response_file = match self.download_response_file(&lock_response.response_locator).await {
                    Ok(response) => {
                        info!(
                            "Verifier downloaded a response file {} of size {}",
                            lock_response.response_locator,
                            response.len()
                        );
                        response
                    }
                    Err(err) => {
                        info!(
                            "Verifier failed to download a response file {} (error: {})",
                            lock_response.response_locator, err
                        );
                        continue;
                    }
                };

                // Update the .verified to .unverified in the response locator
                let challenge_file_locator = lock_response.response_locator.replace(".unverified", ".verified");

                trace!("challenge_file_locator: {}", challenge_file_locator);
                trace!("response_locator: {}", lock_response.response_locator);
                trace!("next_challenge_locator: {}", lock_response.next_challenge_locator);

                // Download the challenge file from the coordinator
                let challenge_file = match self.download_challenge_file(&challenge_file_locator).await {
                    Ok(response) => {
                        info!(
                            "Verifier downloaded a challenge file {} of size {}",
                            challenge_file_locator,
                            response.len()
                        );
                        response
                    }
                    Err(err) => {
                        error!(
                            "Verifier failed to download a response file {} (error: {})",
                            challenge_file_locator, err
                        );
                        continue;
                    }
                };

                // Write the challenge and response files to disk

                info!(
                    "Writing the response file (size: {}) {} to disk",
                    response_file.len(),
                    &lock_response.response_locator
                );
                let response_locator_path = std::path::Path::new(&lock_response.response_locator);
                let response_locator_parent = response_locator_path.parent().unwrap();
                std::fs::create_dir_all(response_locator_parent).unwrap();
                if let Err(err) = std::fs::write(response_locator_path, response_file) {
                    error!(
                        "Error writing response file to path {} (error: {})",
                        &lock_response.response_locator, err
                    );
                }

                info!(
                    "Writing the challenge file (size: {}) {} to disk",
                    challenge_file.len(),
                    &challenge_file_locator
                );
                let challenge_locator_path = std::path::Path::new(&challenge_file_locator);
                let challenge_locator_path_parent = challenge_locator_path.parent().unwrap();
                std::fs::create_dir_all(challenge_locator_path_parent).unwrap();
                if let Err(err) = std::fs::write(challenge_locator_path, challenge_file) {
                    error!(
                        "Error writing challenge file to path {}  (error: {})",
                        &challenge_file_locator, err
                    );
                }

                // Run the verification

                let settings = self.environment.to_settings();
                let (_, _, curve, _, _, _) = settings.clone();

                let compressed_challenge = self.environment.compressed_inputs();
                let compressed_response = self.environment.compressed_outputs();

                let chunk_id = lock_response.chunk_id;

                match curve {
                    CurveKind::Bls12_377 => transform_pok_and_correctness(
                        compressed_challenge,
                        &challenge_file_locator,
                        compressed_response,
                        &lock_response.response_locator,
                        compressed_challenge,
                        &lock_response.next_challenge_locator,
                        &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                    ),
                    CurveKind::BW6 => transform_pok_and_correctness(
                        compressed_challenge,
                        &challenge_file_locator,
                        compressed_response,
                        &lock_response.response_locator,
                        compressed_challenge,
                        &lock_response.next_challenge_locator,
                        &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                    ),
                };

                let next_challenge_file = match std::fs::read(&lock_response.next_challenge_locator) {
                    Ok(file) => file,
                    Err(err) => {
                        error!(
                            "Failed to open next_challenge_file at {} (error {})",
                            &lock_response.next_challenge_locator, err
                        );
                        continue;
                    }
                };

                // Upload the verified response file to `lock_response.response.locator`

                match self
                    .upload_next_challenge_locator_file(&lock_response.next_challenge_locator, next_challenge_file)
                    .await
                {
                    Ok(response) => {
                        info!(
                            "Verifier uploaded the new verification challenge file. Response {}",
                            response
                        );
                    }
                    Err(err) => {
                        error!(
                            "Failed to open upload next challenge {} to coordinator (error: {})",
                            &lock_response.next_challenge_locator, err
                        );
                        continue;
                    }
                };

                // Attempt to apply the uploaded response locator
                match self.verify_contribution(&lock_response.next_challenge_locator).await {
                    Ok(response) => info!(
                        "Verifier verified the response file {}. Response {}",
                        lock_response.next_challenge_locator, response
                    ),
                    Err(err) => {
                        error!(
                            "Failed to run verification on the next challenge {} (error: {})",
                            &lock_response.next_challenge_locator, err
                        );
                        continue;
                    }
                }
            }

            // Sleep for 10 seconds
            sleep(Duration::from_secs(10));
        }
    }
}
