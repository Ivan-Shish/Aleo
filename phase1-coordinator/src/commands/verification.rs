use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageLock},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters, PublicKey};
use setup_utils::{calculate_hash, CheckForCorrectness, GenericArray, U64};

use std::io::Write;
use tracing::{debug, error, info, trace};
use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

#[allow(dead_code)]
pub(crate) struct Verification;

impl Verification {
    ///
    /// Runs verification for a given environment, storage,
    /// round height, chunk ID, and contribution ID of the
    /// unverified response file.
    ///
    #[inline]
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageLock,
        round_height: u64,
        chunk_id: u64,
        current_contribution_id: u64,
        is_final_contribution: bool,
    ) -> Result<(), CoordinatorError> {
        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && current_contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero);
        }

        // Check that the chunk ID is valid.
        if chunk_id > environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the locators for `Verification`.
        let challenge = Locator::ContributionFile(round_height, chunk_id, current_contribution_id - 1, true);
        let response = Locator::ContributionFile(round_height, chunk_id, current_contribution_id, false);
        let next_challenge = match is_final_contribution {
            true => Locator::ContributionFile(round_height + 1, chunk_id, 0, true),
            false => Locator::ContributionFile(round_height, chunk_id, current_contribution_id, true),
        };

        trace!("Challenge locator is {}", storage.to_path(&challenge)?);
        trace!("Response locator is {}", storage.to_path(&response)?);
        trace!("Next challenge locator is {}", storage.to_path(&next_challenge)?);

        if let Err(error) = Self::verification(environment, storage, chunk_id, challenge, response, next_challenge) {
            error!("Verification failed with {}", error);
            return Err(error);
        }

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    fn verification(
        environment: &Environment,
        storage: &mut StorageLock,
        chunk_id: u64,
        challenge: Locator,
        response: Locator,
        next_challenge: Locator,
    ) -> Result<(), CoordinatorError> {
        // Check that the previous and current locators exist in storage.
        if !storage.exists(&challenge) || !storage.exists(&response) {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Execute ceremony verification on chunk.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings.clone();
        let result = match curve {
            CurveKind::Bls12_377 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge)?.as_ref(),
                storage.reader(&response)?.as_ref(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge)?.as_ref(),
                storage.reader(&response)?.as_ref(),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
            ),
        };
        let response_hash = match result {
            Ok(response_hash) => response_hash,
            Err(error) => {
                error!("Verification failed with {}", error);
                return Err(CoordinatorError::VerificationFailed.into());
            }
        };

        trace!("Verification succeeded! Writing the next challenge file");

        // Fetch the compression settings.
        let response_is_compressed = environment.compressed_outputs();
        let next_challenge_is_compressed = environment.compressed_inputs();

        // Create the next challenge file.
        let next_challenge_hash = if response_is_compressed == next_challenge_is_compressed {
            // TODO (howardwu): Update this.
            trace!("Copying decompressed response file without the public key");
            storage.copy(&response, &next_challenge)?;

            calculate_hash(&storage.reader(&next_challenge)?)
        } else {
            trace!("Starting decompression of the response file for the next challenge file");

            // Initialize the next contribution locator, if it does not exist.
            if !storage.exists(&next_challenge) {
                storage.initialize(
                    next_challenge.clone(),
                    Object::contribution_file_size(environment, chunk_id, true),
                )?;
            }

            let (_, _, curve, _, _, _) = settings.clone();
            match curve {
                CurveKind::Bls12_377 => Self::decompress(
                    storage.reader(&response)?.as_ref(),
                    storage.writer(&next_challenge)?.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                )?,
                CurveKind::BW6 => Self::decompress(
                    storage.reader(&response)?.as_ref(),
                    storage.writer(&next_challenge)?.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                )?,
            };

            calculate_hash(storage.reader(&next_challenge)?.as_ref())
        };

        debug!("The next challenge hash is {}", pretty_hash!(&next_challenge_hash));

        {
            // Fetch the saved response hash in the next challenge file.
            let saved_response_hash = storage
                .reader(&next_challenge)?
                .as_ref()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();

            // Check that the response hash matches the next challenge hash.
            debug!("The response hash is {}", pretty_hash!(&response_hash));
            debug!("The saved response hash is {}", pretty_hash!(&saved_response_hash));
            if response_hash.as_slice() != saved_response_hash {
                error!("Response hash does not match the saved response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    fn transform_pok_and_correctness<T: Engine + Sync>(
        environment: &Environment,
        challenge_reader: &[u8],
        response_reader: &[u8],
        parameters: &Phase1Parameters<T>,
    ) -> Result<GenericArray<u8, U64>, CoordinatorError> {
        debug!("Verifying 2^{} powers of tau", parameters.total_size_in_log2);

        // Check that the challenge hashes match.
        let challenge_hash = {
            // Compute the challenge hash using the challenge file.
            let challenge_hash = calculate_hash(challenge_reader.as_ref());

            // Fetch the challenge hash from the response file.
            let saved_challenge_hash = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];

            // Check that the challenge hashes match.
            debug!("The challenge hash is {}", pretty_hash!(&challenge_hash));
            debug!("The saved challenge hash is {}", pretty_hash!(&saved_challenge_hash));
            match challenge_hash.as_slice() == saved_challenge_hash {
                true => challenge_hash,
                false => {
                    error!("Challenge hash does not match saved challenge hash.");
                    return Err(CoordinatorError::ContributionHashMismatch);
                }
            }
        };

        // Compute the response hash using the response file.
        let response_hash = calculate_hash(response_reader);

        // Fetch the compression settings.
        let compressed_challenge = environment.compressed_inputs();
        let compressed_response = environment.compressed_outputs();

        // Fetch the public key of the contributor.
        let public_key = PublicKey::read(response_reader, compressed_response, &parameters)?;
        // trace!("Public key of the contributor is {:#?}", public_key);

        trace!("Starting verification");
        Phase1::verification(
            challenge_reader,
            response_reader,
            &public_key,
            &challenge_hash,
            compressed_challenge,
            compressed_response,
            CheckForCorrectness::No,
            CheckForCorrectness::Full,
            &parameters,
        )?;
        trace!("Completed verification");

        Ok(response_hash)
    }

    #[allow(dead_code)]
    #[inline]
    fn decompress<'a, T: Engine + Sync>(
        response_reader: &[u8],
        mut next_challenge_writer: &mut [u8],
        response_hash: &[u8],
        parameters: &Phase1Parameters<T>,
    ) -> Result<(), CoordinatorError> {
        {
            (&mut next_challenge_writer[0..]).write_all(response_hash)?;
            next_challenge_writer.flush()?;
        }

        trace!("Decompressing the response file for the next challenge");
        Phase1::decompress(
            response_reader,
            next_challenge_writer,
            CheckForCorrectness::No,
            &parameters,
        )?;
        next_challenge_writer.flush()?;
        trace!("Decompressed the response file for the next challenge");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Verification},
        storage::{Locator, Object, StorageLock},
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;

    #[test]
    #[serial]
    fn test_verification_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone()).unwrap();
        let test_storage = coordinator.storage();

        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height().unwrap());

        // Run initialization.
        coordinator
            .next_round(*TEST_STARTED_AT, vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()], vec![
                Lazy::force(&TEST_VERIFIER_ID).clone(),
            ])
            .unwrap();

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height().unwrap());

        // Define test parameters.
        let round_height = coordinator.current_round_height().unwrap();
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();
        let is_final = true;

        // Obtain the storage lock.
        let mut storage = StorageLock::Write(test_storage.write().unwrap());

        for chunk_id in 0..number_of_chunks {
            // Fetch the challenge locator.
            let challenge_locator = &Locator::ContributionFile(round_height, chunk_id, 0, true);
            // Fetch the response locator.
            let response_locator = &Locator::ContributionFile(round_height, chunk_id, 1, false);
            if !storage.exists(response_locator) {
                let expected_filesize = Object::contribution_file_size(&TEST_ENVIRONMENT_3, chunk_id, false);
                storage.initialize(response_locator.clone(), expected_filesize).unwrap();
            }

            // Run computation on chunk.
            Computation::run(&TEST_ENVIRONMENT_3, &mut storage, challenge_locator, response_locator).unwrap();

            // Run verification on chunk.
            Verification::run(&TEST_ENVIRONMENT_3, &mut storage, round_height, chunk_id, 1, is_final).unwrap();

            // Fetch the next contribution locator.
            let next = match is_final {
                true => Locator::ContributionFile(round_height + 1, chunk_id, 0, true),
                false => Locator::ContributionFile(round_height, chunk_id, 1, true),
            };

            // Check the next challenge file exists.
            assert!(storage.exists(&next));
        }
    }
}

// info!("Sanity checking contribution 0 in round {}", round_height);
//
// // Open the transcript file.
// let transcript = environment.contribution_locator(round_height, chunk_id, contribution_id);
// let file = OpenOptions::new().read(true).open(&transcript)?;
// let reader = unsafe { MmapOptions::new().map(&file)? };
//
// // Check that the contribution chunk was generated based on the blank hash.
// let hash = blank_hash();
// for (expected, candidate) in hash.iter().zip(reader.chunks(64).next().unwrap()) {
//     if expected != candidate {
//         return Err(CoordinatorError::ChunkVerificationFailed.into());
//     }
// }
//
// // Compute the contribution hash to ensure it works.
// let contribution_hash = calculate_hash(&reader);
//
// if round_height == 0 {
//     Self::copy_initial(environment, round_height, chunk_id, contribution_hash)?;
// }
//
// info!("Completed sanity checking of contribution 0 in round {}", round_height);
// return Ok(());
