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
    /// Runs computation for a given environment, round height, chunk ID, and contribution ID.
    ///
    /// Executes the round computation on a given chunk ID and contribution ID.
    ///
    #[allow(dead_code)]
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageLock,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> anyhow::Result<()> {
        info!(
            "Starting computation on round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );
        let start = Instant::now();

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch a contribution challenge locator.
        let current_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id - 1, true);

        // Initialize and fetch a contribution response locator.
        let next_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id, false);
        if !storage.exists(next_locator) {
            let expected_contribution_size = Object::contribution_file_size(environment, chunk_id, false);
            storage.initialize(next_locator.clone(), expected_contribution_size)?;
        }

        // Execute computation on chunk.
        let (_, _, curve, _, _, _) = settings;
        if let Err(error) = match curve {
            CurveKind::Bls12_377 => Self::contribute(
                environment,
                storage.reader(current_locator)?.as_ref(),
                storage.writer(next_locator)?.as_mut(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                &mut thread_rng(),
            ),
            CurveKind::BW6 => Self::contribute(
                environment,
                storage.reader(current_locator)?.as_ref(),
                storage.writer(next_locator)?.as_mut(),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                &mut thread_rng(),
            ),
        } {
            error!("Computation failed with {}", error);
            return Err(CoordinatorError::ComputationFailed.into());
        }

        // Load a contribution response reader.
        let reader = storage.reader(next_locator)?;
        let contribution_hash = calculate_hash(reader.as_ref());
        debug!("Response hash is {}", pretty_hash!(&contribution_hash));
        debug!("Thank you for your contribution!");

        let elapsed = Instant::now().duration_since(start);
        info!(
            "Completed computation on round {} chunk {} contribution {} in {:?}",
            round_height, chunk_id, contribution_id, elapsed
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
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Initialization},
        storage::{Locator, StorageLock},
        testing::prelude::*,
    };

    use tracing::{debug, trace};

    #[test]
    #[serial]
    fn test_computation_run() {
        initialize_test_environment();

        // Define test parameters.
        let round_height = 0;
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();

        // Define test storage.
        let test_storage = test_storage(&TEST_ENVIRONMENT_3);
        let mut storage = StorageLock::Write(test_storage.write().unwrap());

        // Generate a new challenge for the given parameters.
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Run initialization on chunk.
            let contribution_hash =
                Initialization::run(&TEST_ENVIRONMENT_3, &mut storage, round_height, chunk_id).unwrap();

            // Run computation on chunk.
            Computation::run(&TEST_ENVIRONMENT_3, &mut storage, round_height, chunk_id, 1).unwrap();

            // Fetch the current contribution locator.
            let locator = Locator::ContributionFile(round_height, chunk_id, 1, false);
            let reader = storage.reader(&locator).unwrap();

            // Check that the current contribution was generated based on the previous contribution hash.
            for (i, (expected, candidate)) in contribution_hash
                .iter()
                .zip(reader.as_ref().chunks(64).next().unwrap())
                .enumerate()
            {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }
        }
    }
}
