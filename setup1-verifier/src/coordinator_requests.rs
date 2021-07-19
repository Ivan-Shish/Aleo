use crate::{errors::VerifierError, objects::LockResponse, utils::AleoAuthentication, verifier::Verifier};
use snarkos_toolkit::account::Address;

use reqwest::Client;
use tracing::{debug, error, info};

impl Verifier {
    ///
    /// Attempts to join the coordinator queue
    ///
    /// On success, this function returns `true`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn join_queue(&self) -> Result<bool, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;

        let aleo_address = Address::from_view_key(&self.view_key)?.to_string();

        let method = "post";
        let path = "/v1/queue/verifier/join";

        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &path)?;

        info!("Attempting to join as verifier join the queue as {}", aleo_address);

        match Client::new()
            .post(coordinator_api_url.join(path).expect("Should create a path"))
            .header(http::header::AUTHORIZATION, authentication.to_string())
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Verifier failed to join the queue");
                    return Err(VerifierError::FailedToJoinQueue);
                }

                // Parse the lock response
                let queue_response = serde_json::from_slice::<bool>(&*response.bytes().await?)?;
                info!("{} joined the queue with status {}", aleo_address, queue_response);
                Ok(queue_response)
            }
            Err(_) => {
                error!("Request ({}) to join the queue failed", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to acquire the lock on a chunk.
    ///
    /// On success, this function returns the `LockResponse`.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn lock_chunk(&self) -> Result<LockResponse, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = "/v1/verifier/try_lock";

        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &path)?;

        info!("Verifier attempting to lock a chunk");

        match Client::new()
            .post(coordinator_api_url.join(path).expect("Should create a path"))
            .header(http::header::AUTHORIZATION, authentication.to_string())
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Verifier failed to acquire a lock on a chunk");
                    return Err(VerifierError::FailedLock);
                }

                // Parse the lock response
                let json_response = response.bytes().await?;
                let lock_response = serde_json::from_slice::<LockResponse>(&*json_response)?;
                debug!("Decoded verifier lock response: {:#?}", lock_response);
                info!("Verifier locked chunk {}", lock_response.chunk_id);

                Ok(lock_response)
            }
            Err(_) => {
                error!("Request ({}) to lock a chunk failed", path);
                return Err(VerifierError::FailedRequest(
                    path.to_string(),
                    coordinator_api_url.to_string(),
                ));
            }
        }
    }

    ///
    /// Attempts to run verification in the current round for a given `chunk_id`
    ///
    /// This assumes that a valid challenge file has already been uploaded to the
    /// coordinator at the given `verified_locator`.
    ///
    /// On success, the coordinator returns an { "status": "ok" } response.
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn verify_contribution(&self, chunk_id: u64) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/v1/verifier/try_verify/{}", chunk_id);

        info!("Verifier running verification of a contribution at chunk {}", chunk_id);

        let signature_path = format!("{}", path.replace("./", ""));
        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &signature_path)?;
        match Client::new()
            .post(coordinator_api_url.join(&path).expect("Should create a path"))
            .header(http::header::AUTHORIZATION, authentication.to_string())
            .header(http::header::CONTENT_LENGTH, 0)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to verify the challenge at chunk {}", chunk_id);
                    return Err(VerifierError::FailedVerification(chunk_id));
                }

                info!("Verifier successfully verified a contribution on chunk {}", chunk_id);

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
    pub(crate) async fn download_response_file(
        &self,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/v1/download/response/{}/{}", chunk_id, contribution_id);

        info!("Verifier downloading a response file at {} ", path);

        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &path)?;
        match Client::new()
            .get(coordinator_api_url.join(&path).expect("Should create a path"))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the response file {}", path);
                    return Err(VerifierError::FailedResponseDownload(path));
                }

                info!("Verifier downloaded the response file {} ", path);

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
    pub(crate) async fn download_challenge_file(
        &self,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<Vec<u8>, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "get";
        let path = format!("/v1/download/challenge/{}/{}", chunk_id, contribution_id);

        info!("Verifier downloading a challenge file at {} ", path);

        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &path)?;
        match Client::new()
            .get(coordinator_api_url.join(&path).expect("Should create a path"))
            .header("Authorization", authentication.to_string())
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to download the challenge file {}", path);
                    return Err(VerifierError::FailedChallengeDownload(path));
                }

                info!("Verifier downloaded the challenge file {} ", path);

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
    /// Attempts to upload the next challenge file to the coordinator
    /// at a given `next_challenge_locator`
    ///
    /// On success, this function returns an `ok` status
    ///
    /// On failure, this function returns a `VerifierError`.
    ///
    pub(crate) async fn upload_next_challenge_locator_file(
        &self,
        chunk_id: u64,
        contribution_id: u64,
        signature_and_next_challenge_file_bytes: Vec<u8>,
    ) -> Result<String, VerifierError> {
        let coordinator_api_url = &self.coordinator_api_url;
        let method = "post";
        let path = format!("/v1/upload/challenge/{}/{}", chunk_id, contribution_id);

        let authentication = AleoAuthentication::authenticate(&self.view_key, &method, &path)?;

        info!(
            "Verifier uploading a response with size {} to {} ",
            signature_and_next_challenge_file_bytes.len(),
            path
        );

        match Client::new()
            .post(coordinator_api_url.join(&path).expect("Should create a path"))
            .header(http::header::AUTHORIZATION, authentication.to_string())
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .header(
                http::header::CONTENT_LENGTH,
                signature_and_next_challenge_file_bytes.len(),
            )
            .body(signature_and_next_challenge_file_bytes)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Failed to upload the new challenge file {}", path);
                    return Err(VerifierError::FailedChallengeUpload(path));
                }

                info!("Verifier uploaded the next challenge file {} ", path);

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
}
