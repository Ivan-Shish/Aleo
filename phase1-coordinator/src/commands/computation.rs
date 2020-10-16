use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageLock},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use setup_utils::{calculate_hash, UseCompression};

use rand::{thread_rng, Rng};
use std::{io::Write, time::Instant};
use tracing::{debug, error, info, trace};
use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

#[allow(dead_code)]
pub(crate) struct Computation;

impl Computation {
    ///
    /// Runs computation for a given environment, storage writer, challenge locator,
    /// and response locator.
    ///
    /// This function assumes that the locator for the previous response file, challenge file,
    /// and response file have been initialized, typically as part of a call to
    /// `Coordinator::try_lock` to lock the contribution chunk.
    ///
    #[allow(dead_code)]
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageLock,
        challenge_locator: &Locator,
        response_locator: &Locator,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        info!(
            "Starting computation for\n\n\tChallenge: {}\n\tResponse : {}\n",
            storage.to_path(challenge_locator)?,
            storage.to_path(response_locator)?
        );

        // Fetch the chunk ID and contribution ID from the response locator.
        let (chunk_id, contribution_id) = match response_locator {
            Locator::ContributionFile(_, chunk_id, contribution_id, _) => (*chunk_id as usize, *contribution_id),
            _ => return Err(CoordinatorError::ContributionLocatorIncorrect.into()),
        };

        // if !storage.exists(response_locator) {
        //     let expected_contribution_size = Object::contribution_file_size(environment, chunk_id as u64, false);
        //     storage.initialize(response_locator.clone(), expected_contribution_size)?;
        // }

        // // Check that the saved previous response hash matches the actual previous response hash.
        // Self::check_hash(storage, previous_response_locator, challenge_locator, contribution_id)?;

        // Run computation on chunk.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        if let Err(error) = match curve {
            CurveKind::Bls12_377 => Self::contribute(
                environment,
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                &mut thread_rng(),
            ),
            CurveKind::BW6 => Self::contribute(
                environment,
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                &mut thread_rng(),
            ),
        } {
            error!("Computation failed with {}", error);
            return Err(CoordinatorError::ComputationFailed.into());
        }

        // Load a contribution response reader.
        let reader = storage.reader(response_locator)?;
        let contribution_hash = calculate_hash(reader.as_ref());
        debug!("Response hash is {}", pretty_hash!(&contribution_hash));

        let elapsed = Instant::now().duration_since(start);
        info!(
            "Completed computation on {} in {:?}",
            storage.to_path(response_locator)?,
            elapsed
        );
        Ok(())
    }

    #[allow(dead_code)]
    fn contribute<T: Engine + Sync>(
        environment: &Environment,
        challenge_reader: &[u8],
        mut response_writer: &mut [u8],
        parameters: &Phase1Parameters<T>,
        mut rng: impl Rng,
    ) -> Result<(), CoordinatorError> {
        // Fetch the environment settings.
        let compressed_inputs = environment.compressed_inputs();
        let compressed_outputs = environment.compressed_outputs();
        let check_input_for_correctness = environment.check_input_for_correctness();

        // Check that the challenge hash is not compressed.
        if UseCompression::Yes == compressed_inputs {
            error!("Compressed contribution hashing is currently not supported");
            return Err(CoordinatorError::CompressedContributionHashingUnsupported);
        }

        trace!("Calculating previous contribution hash and writing it to the response");
        let challenge_hash = calculate_hash(challenge_reader);
        debug!("Challenge hash is {}", pretty_hash!(&challenge_hash));
        (&mut response_writer[0..]).write_all(challenge_hash.as_slice())?;
        response_writer.flush()?;

        let previous_hash = &challenge_reader
            .get(0..64)
            .ok_or(CoordinatorError::StorageReaderFailed)?;
        debug!("Challenge file claims previous hash is {}", pretty_hash!(previous_hash));
        debug!("Please double check this yourself! Do not trust it blindly!");

        // Construct our keypair using the RNG we created above.
        let (public_key, private_key) =
            Phase1::key_generation(&mut rng, challenge_hash.as_ref()).expect("could not generate keypair");

        // Perform the transformation
        trace!("Computing and writing your contribution, this could take a while");
        Phase1::computation(
            challenge_reader,
            response_writer,
            compressed_inputs,
            compressed_outputs,
            check_input_for_correctness,
            &private_key,
            &parameters,
        )?;
        response_writer.flush()?;
        trace!("Finishing writing your contribution to response file");

        // Write the public key.
        public_key.write(response_writer, compressed_outputs, &parameters)?;

        Ok(())
    }

    // /// Compute both contribution hashes and check for equivalence.
    // #[inline]
    // fn check_hash(
    //     storage: &StorageLock,
    //     previous_response_locator: &Locator,
    //     challenge_locator: &Locator,
    //     contribution_id: u64,
    // ) -> anyhow::Result<Vec<u8>> {
    //     trace!("Checking the previous response locator hash");
    //
    //     // Compare the contribution hashes of both files to confirm the hash chain.
    //     let previous_response_reader = storage.reader(previous_response_locator)?;
    //     let previous_response_hash = {
    //         let computed_hash = calculate_hash(previous_response_reader.as_ref());
    //         debug!("The hash of previous response file is {}", pretty_hash!(computed_hash));
    //         computed_hash
    //     };
    //
    //     let challenge_reader = storage.reader(challenge_locator)?;
    //     let previous_response_hash_saved = {
    //         let saved_hash = challenge_reader
    //             .get(0..64)
    //             .ok_or(CoordinatorError::StorageReaderFailed)?;
    //         debug!(
    //             "The saved hash of previous response file is {}",
    //             pretty_hash!(saved_hash)
    //         );
    //         saved_hash
    //     };
    //
    //     debug!("Please double check this!");
    //     if previous_response_hash.as_slice() != previous_response_hash_saved {
    //         error!(
    //             "The hash of the previous response file does not match the saved hash of the previous response file in the challenge file"
    //         );
    //         return Err(CoordinatorError::InitializationTranscriptsDiffer.into());
    //     }
    //
    //     Ok(previous_response_hash.to_vec())
    // }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Initialization},
        storage::{Locator, Object, StorageLock},
        testing::prelude::*,
    };
    use setup_utils::calculate_hash;

    use tracing::{debug, error, trace};

    #[test]
    #[serial]
    fn test_computation_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        // Define test parameters.
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();
        let expected_number_of_contributions = 1;

        // Define test storage.
        let test_storage = test_storage(&TEST_ENVIRONMENT_3);
        let mut storage = StorageLock::Write(test_storage.write().unwrap());

        // Generate a new challenge for the given parameters.
        let round_height = 0;
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Run initialization on chunk.
            Initialization::run(&TEST_ENVIRONMENT_3, &mut storage, round_height, chunk_id).unwrap();
        }

        // Generate a new challenge for the given parameters.
        let round_height = 1;
        for chunk_id in 0..number_of_chunks {
            trace!("Running computation on test chunk {}", chunk_id);

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

            // Check that the current contribution was generated based on the previous contribution hash.
            let challenge_hash = calculate_hash(&storage.reader(&challenge_locator).unwrap());
            let saved_challenge_hash = storage
                .reader(&response_locator)
                .unwrap()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();
            for (i, (expected, candidate)) in (challenge_hash.iter().zip(&saved_challenge_hash)).enumerate() {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }
        }
    }
}
