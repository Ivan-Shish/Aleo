use crate::{
    errors::VerifierError,
    utils::{create_parent_directory, write_to_file},
};
use phase1::helpers::CurveKind;
use phase1_cli::transform_pok_and_correctness;
use phase1_coordinator::{environment::Environment, phase1_chunked_parameters, Participant};
use setup_utils::calculate_hash;
use snarkos_toolkit::account::{Address, ViewKey};
use zexe_algebra::{Bls12_377, BW6_761};

use std::{fs, str::FromStr, thread::sleep, time::Duration};
use tracing::{debug, error, info, trace};

/// Returns a pretty print of the given hash bytes for logging.
macro_rules! pretty_hash {
    ($hash:expr) => {{
        let mut output = format!("\n\n");
        for line in $hash.chunks(16) {
            output += "\t";
            for section in line.chunks(4) {
                for b in section {
                    output += &format!("{:02x}", b);
                }
                output += " ";
            }
            output += "\n";
        }
        output
    }};
}

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
            // TODO (raychu86): Modularize the verifier logic into a more digestible layout.

            // TODO (raychu86): The sleep is currently at the top of the loop so it won't be
            //  skipped by the `continue` calls. This will be updated in the verifier refactor.
            // Sleep for 10 seconds
            sleep(Duration::from_secs(10));

            // Attempt to join the queue
            if let Ok(response) = self.join_queue().await {
                info!("Verifier {} joined the queue with status {}", self.verifier, response)
            }

            // Attempt to lock a chunk
            if let Ok(lock_response) = self.lock_chunk().await {
                // Extract the lock response attributes
                let chunk_id = lock_response.chunk_id;
                let challenge_file_locator = lock_response.challenge_locator;
                let response_locator = lock_response.response_locator;
                let next_challenge_locator = lock_response.next_challenge_locator;

                info!("Verifier locked a chunk {} ", chunk_id);
                trace!("challenge_file_locator: {}", challenge_file_locator);
                trace!("response_locator: {}", response_locator);
                trace!("next_challenge_locator: {}", next_challenge_locator);

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
                    "Writing the challenge file (size: {}) {} to disk",
                    challenge_file.len(),
                    &challenge_file_locator
                );
                write_to_file(&challenge_file_locator, challenge_file);

                // Download the response file from the coordinator
                let response_file = match self.download_response_file(&response_locator).await {
                    Ok(response) => {
                        info!(
                            "Verifier downloaded a response file {} of size {}",
                            response_locator,
                            response.len()
                        );
                        response
                    }
                    Err(err) => {
                        info!(
                            "Verifier failed to download a response file {} (error: {})",
                            response_locator, err
                        );
                        continue;
                    }
                };

                // Write the response to a local file
                info!(
                    "Writing the response file (size: {}) {} to disk",
                    response_file.len(),
                    &response_locator
                );

                // Compute the response hash using the response file.
                let response_hash = calculate_hash(&response_file);

                write_to_file(&response_locator, response_file);

                // Create the parent directory for the `next_challenge_locator`
                info!(
                    "Initializing the next challenge locator directory: {}",
                    next_challenge_locator
                );
                create_parent_directory(&next_challenge_locator);

                // Run the verification

                let settings = self.environment.parameters();
                let (_, _, curve, _, _, _) = settings.clone();

                let compressed_challenge = self.environment.compressed_inputs();
                let compressed_response = self.environment.compressed_outputs();

                info!("Running verification on chunk {}", chunk_id);

                match curve {
                    CurveKind::Bls12_377 => transform_pok_and_correctness(
                        compressed_challenge,
                        &challenge_file_locator,
                        compressed_response,
                        &response_locator,
                        compressed_challenge,
                        &next_challenge_locator,
                        &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                    ),
                    CurveKind::BW6 => transform_pok_and_correctness(
                        compressed_challenge,
                        &challenge_file_locator,
                        compressed_response,
                        &response_locator,
                        compressed_challenge,
                        &next_challenge_locator,
                        &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                    ),
                };

                // Reads the file at the `next_challenge_locator` that was generated
                // by the verification step
                let next_challenge_file = match fs::read(&next_challenge_locator) {
                    Ok(file) => {
                        info!("Reading next challenge locator at {}", &next_challenge_locator);
                        file
                    }
                    Err(err) => {
                        error!(
                            "Failed to open next_challenge_file at {} (error {})",
                            &next_challenge_locator, err
                        );
                        continue;
                    }
                };

                let next_challenge_hash = calculate_hash(&next_challenge_file);
                debug!("The next challenge hash is {}", pretty_hash!(&next_challenge_hash));

                info!("Check that the response hash matches the next challenge hash");

                {
                    // Fetch the saved response hash in the next challenge file.
                    let saved_response_hash = next_challenge_file.chunks(64).next().unwrap().to_vec();

                    // Check that the response hash matches the next challenge hash.
                    debug!("The response hash is {}", pretty_hash!(&response_hash));
                    debug!("The saved response hash is {}", pretty_hash!(&saved_response_hash));
                    if response_hash.as_slice() != saved_response_hash {
                        error!("Response hash does not match the saved response hash.");
                        continue;
                    }
                }

                // Upload the new challenge file to `next_challenge_locator`
                match self
                    .upload_next_challenge_locator_file(&next_challenge_locator, next_challenge_file)
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
                            &next_challenge_locator, err
                        );
                        continue;
                    }
                };

                // Attempt to perform the verification with the uploaded challenge file at `next_challenge_locator`
                match self.verify_contribution(chunk_id).await {
                    Ok(response) => info!(
                        "Verifier verified the response at chunk id {}. Response {}",
                        chunk_id, response
                    ),
                    Err(err) => {
                        error!(
                            "Failed to verify the response at chunk id {} (error: {})",
                            &chunk_id, err
                        );
                        continue;
                    }
                }
            }
        }
    }
}
