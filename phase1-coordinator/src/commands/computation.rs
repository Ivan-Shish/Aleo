use crate::{
    environment::Environment,
    storage::{Locator, StorageLock},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use setup_utils::{calculate_hash, derive_rng_from_seed, UseCompression};

use rand::Rng;
use std::{io::Write, time::Instant};
use tracing::{debug, error, info, trace};
use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

pub const SEED_LENGTH: usize = 32;
pub type Seed = [u8; SEED_LENGTH];

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
        seed: &Seed,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        info!(
            "Starting computation for\n\n\tChallenge: {}\n\tResponse : {}\n",
            storage.to_path(challenge_locator)?,
            storage.to_path(response_locator)?
        );

        // Fetch the chunk ID from the response locator.
        let chunk_id = match response_locator {
            Locator::ContributionFile(_, chunk_id, _, _) => *chunk_id as usize,
            _ => return Err(CoordinatorError::ContributionLocatorIncorrect.into()),
        };

        // Run computation on chunk.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        if let Err(error) = match curve {
            CurveKind::Bls12_377 => Self::contribute(
                environment,
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                derive_rng_from_seed(&seed[..]),
            ),
            CurveKind::BW6 => Self::contribute(
                environment,
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                derive_rng_from_seed(&seed[..]),
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
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Initialization},
        storage::{Locator, Object, StorageLock},
        testing::prelude::*,
    };
    use setup_utils::calculate_hash;

    use crate::commands::{Seed, SEED_LENGTH};
    use rand::RngCore;
    use tracing::{debug, trace};

    #[test]
    #[serial]
    fn test_computation_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        // Define test parameters.
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();

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
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            Computation::run(
                &TEST_ENVIRONMENT_3,
                &mut storage,
                challenge_locator,
                response_locator,
                &seed,
            )
            .unwrap();

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
