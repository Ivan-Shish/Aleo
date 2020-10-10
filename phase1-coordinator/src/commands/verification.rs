use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageWrite},
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
        storage: &mut StorageWrite,
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

        // Fetch the contribution locators for `Verification`.
        let previous = Locator::ContributionFile(round_height, chunk_id, current_contribution_id - 1, true);
        let current = Locator::ContributionFile(round_height, chunk_id, current_contribution_id, false);
        let next = match is_final_contribution {
            true => Locator::ContributionFile(round_height + 1, chunk_id, 0, true),
            false => Locator::ContributionFile(round_height, chunk_id, current_contribution_id, true),
        };

        trace!("Previous contribution locator is {}", storage.to_path(&previous)?);
        trace!("Current contribution locator is {}", storage.to_path(&current)?);
        trace!("Next contribution locator is {}", storage.to_path(&next)?);

        if let Err(error) = Self::verification(environment, storage, chunk_id, previous, current, next) {
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
        storage: &mut StorageWrite,
        chunk_id: u64,
        challenge_file: Locator,
        response_file: Locator,
        next_challenge_file: Locator,
    ) -> Result<(), CoordinatorError> {
        // Check that the previous and current locators exist in storage.
        if !storage.exists(&challenge_file) || !storage.exists(&response_file) {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Check that the next contribution locator does not exist.
        if storage.exists(&next_challenge_file) {
            return Err(CoordinatorError::ContributionLocatorAlreadyExists);
        }

        // Execute ceremony verification on chunk.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings.clone();
        let result = match curve {
            CurveKind::Bls12_377 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge_file)?.as_ref(),
                storage.reader(&response_file)?.as_ref(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge_file)?.as_ref(),
                storage.reader(&response_file)?.as_ref(),
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
            trace!("Copying decompressed response file without the public key");
            storage.copy(&response_file, &next_challenge_file)?;

            calculate_hash(&storage.reader(&next_challenge_file)?)
        } else {
            trace!("Starting decompression of the response file for the next challenge file");

            // Initialize the next challenge file.
            storage.initialize(
                next_challenge_file.clone(),
                Object::contribution_file_size(environment, chunk_id, true),
            )?;

            let (_, _, curve, _, _, _) = settings.clone();
            match curve {
                CurveKind::Bls12_377 => Self::decompress(
                    storage.reader(&response_file)?.as_ref(),
                    storage.writer(&next_challenge_file)?.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                )?,
                CurveKind::BW6 => Self::decompress(
                    storage.reader(&response_file)?.as_ref(),
                    storage.writer(&next_challenge_file)?.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                )?,
            };

            calculate_hash(storage.reader(&next_challenge_file)?.as_ref())
        };

        debug!("The next challenge hash is {}", pretty_hash!(&next_challenge_hash));
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

        // Fetch the compression settings.
        let compressed_challenge = environment.compressed_inputs();
        let compressed_response = environment.compressed_outputs();

        // Compute the challenge hash using the challenge file.
        let challenge_hash = calculate_hash(challenge_reader.as_ref());
        debug!("The challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

        // Fetch the challenge hash from the response file.
        let challenge_hash_in_response = &response_reader
            .get(0..64)
            .ok_or(CoordinatorError::StorageReaderFailed)?[..];
        let pretty_hash = pretty_hash!(&challenge_hash_in_response);
        debug!("Challenge hash in response file is {}", pretty_hash);

        // Check that the challenge hashes match.
        if challenge_hash_in_response != challenge_hash.as_slice() {
            error!("Challenge hash in response file does not match the expected challenge hash.");
            return Err(CoordinatorError::ContributionHashMismatch);
        }

        // Compute the response hash using the response file.
        let response_hash = calculate_hash(response_reader);
        debug!("The response hash is {}", pretty_hash!(&response_hash));

        // Fetch the public key of the contributor.
        let public_key = PublicKey::read(response_reader, compressed_response, &parameters)?;
        // debug!("Public key of the contributor is {:#?}", public_key);

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
        storage::Locator,
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;

    #[test]
    #[serial]
    fn test_verification_run() {
        initialize_test_environment();

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
        let mut storage = test_storage.write().unwrap();

        for chunk_id in 0..number_of_chunks {
            // Run computation on chunk.
            Computation::run(&TEST_ENVIRONMENT_3, &mut storage, round_height, chunk_id, 1).unwrap();

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
