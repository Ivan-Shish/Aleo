use crate::{errors::VerifierError, utils::authenticate};
use phase1::helpers::CurveKind;
use phase1_cli::transform_pok_and_correctness;
use phase1_coordinator::{environment::Environment, phase1_chunked_parameters, Participant};
use snarkos_toolkit::account::{Address, ViewKey};
use zexe_algebra::{Bls12_377, BW6_761};

use reqwest::Client;
use std::{str::FromStr, thread::sleep, time::Duration};
use tracing::{debug, error, info, trace};

///
/// This lock response bundles the data required for the verifier
/// to perform a valid verification.
///
#[derive(Debug, Serialize, Deserialize)]
pub struct LockResponse {
    /// The chunk id
    #[serde(alias = "chunkId")]
    pub chunk_id: u64,

    /// Indicator if the chunk was locked
    pub locked: bool,

    /// The participant id related to the lock
    #[serde(alias = "participantID")]
    pub participant_id: String,

    #[serde(alias = "challengeLocator")]
    pub challenge_locator: String,

    #[serde(alias = "responseLocator")]
    pub response_locator: String,

    #[serde(alias = "nextChallengeLocator")]
    pub next_challenge_locator: String,
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
    /// Attempts to acquire the lock on a chunk.
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
                if !response.status().is_success() {
                    error!("Verifier failed to acquire a lock on a chunk");
                    return Err(VerifierError::FailedLock);
                }

                // Parse the lock response
                let json_response = response.json().await?;
                let lock_response = serde_json::from_value::<LockResponse>(json_response)?;
                debug!("Decoded verifier lock response: {:#?}", lock_response);

                Ok(lock_response)
            }
            Err(_) => {
                error!("Request ({}) to lock a chunk.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to run verification in the current round for a given `verified_locator`
    ///
    /// This assumes that a valid challenge file has already been uploaded to the
    /// coordinator at the given `verified_locator`.
    ///
    /// On success, the coordinator returns an { "status": "ok" } response.
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

        let signature_path = format!("/api{}", path.replace("./", ""));
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to verify the challenge {}", verified_locator);
                    return Err(VerifierError::FailedVerification(verified_locator.to_string()));
                }

                Ok(response.text().await?)
            }
            Err(_) => {
                error!("Request ({}) to verify a contribution failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to download the unverified response file from the coordinator at
    /// a given `response_locator`
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

        let signature_path = format!("/api{}", path.replace("./", ""));
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the response file {}", response_locator);
                    return Err(VerifierError::FailedResponseDownload(response_locator.to_string()));
                }

                Ok(response.bytes().await?.to_vec())
            }
            Err(_) => {
                error!("Request ({}) to download a response file failed.", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to download the challenge file from the coordinator at
    /// a given `challenge_locator`
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

        let signature_path = format!("/api{}", path.replace("./", ""));
        let authentication = authenticate(&view_key, &method, &signature_path)?;
        match Client::new()
            .get(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the challenge file {}", challenge_locator);
                    return Err(VerifierError::FailedChallengeDownload(challenge_locator.to_string()));
                }

                Ok(response.bytes().await?.to_vec())
            }
            Err(_) => {
                error!("Request ({}) to download a challenge file failed.", path);
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
        let path = format!("/coordinator/verification/{}", next_challenge_locator);

        let view_key = ViewKey::from_str(&self.view_key)?;

        let signature_path = format!("/api{}", path.replace("./", ""));
        let authentication = authenticate(&view_key, &method, &signature_path)?;

        info!(
            "Verifier uploading a response with size {} to {} ",
            next_challenge_file_bytes.len(),
            next_challenge_locator
        );

        match Client::new()
            .post(&format!("{}{}", &coordinator_api_url, &path))
            .header("Authorization", authentication.to_string())
            .header("Content-Type", "application/octet-stream")
            .body(next_challenge_file_bytes)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to upload the new challenge file {}", next_challenge_locator);
                    return Err(VerifierError::FailedChallengeUpload(next_challenge_locator.to_string()));
                }

                Ok(response.text().await?)
            }
            Err(_) => {
                error!("Request ({}) to upload a new challenge file failed.", path);
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
    /// 1. Attempts to lock a chunk
    /// 2. Downloads the challenge file
    /// 3. Downloads the response file
    /// 4. Runs the verification on these two files
    /// 5. Stores the verification to the new challenge file
    /// 6. Uploads the new challenge file to the coordinator
    /// 7. Attempts to apply the verification in the ceremony
    ///     - Request to the coordinator to run `try_verify`
    ///
    /// After completion, the loop waits 10 seconds and starts again at step 1.
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
                let challenge_file_locator = lock_response.challenge_locator;

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

                info!(
                    "Initializing the next challenge locator directory: {}",
                    lock_response.next_challenge_locator
                );

                let next_challenge_locator_path = std::path::Path::new(&lock_response.next_challenge_locator);
                let next_challenge_locator_path_parent = next_challenge_locator_path.parent().unwrap();
                std::fs::create_dir_all(next_challenge_locator_path_parent).unwrap();

                // Run the verification

                let settings = self.environment.to_settings();
                let (_, _, curve, _, _, _) = settings.clone();

                let compressed_challenge = self.environment.compressed_inputs();
                let compressed_response = self.environment.compressed_outputs();

                let chunk_id = lock_response.chunk_id;

                info!("Running verification on chunk {}", chunk_id);

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
                    Ok(file) => {
                        info!(
                            "Reading next challenge locator at {}",
                            &lock_response.next_challenge_locator
                        );
                        file
                    }
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
