use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageWrite},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use setup_utils::{calculate_hash, UseCompression};

use rand::{thread_rng, Rng};
use std::{
    io::{Read, Write},
    panic,
    time::Instant,
};
use tracing::{debug, info, trace};
use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

pub(crate) struct Computation;

impl Computation {
    ///
    /// Runs computation for a given environment, round height, chunk ID, and contribution ID.
    ///
    /// Executes the round computation on a given chunk ID and contribution ID.
    ///
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageWrite,
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
        let expected_contribution_size = Object::contribution_file_size(environment, chunk_id, false);
        storage.initialize(next_locator.clone(), expected_contribution_size)?;

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
            return Err(CoordinatorError::ComputationFailed.into());
        }

        // Load a contribution response reader.
        let mut reader = storage.reader(next_locator)?;
        let contribution_hash = calculate_hash(reader.as_ref());

        debug!("Done! Your contribution has been written. The BLAKE2b hash of response file is:\n");
        Self::log_hash(&contribution_hash);
        debug!("Thank you for your participation, much appreciated!");

        let elapsed = Instant::now().duration_since(start);
        info!(
            "Completed computation on round {} chunk {} contribution {} in {:?}",
            round_height, chunk_id, contribution_id, elapsed
        );
        Ok(())
    }

    fn contribute<T: Engine + Sync>(
        environment: &Environment,
        reader: &[u8],
        mut writer: &mut [u8],
        parameters: &Phase1Parameters<T>,
        mut rng: impl Rng,
    ) -> Result<(), CoordinatorError> {
        // Fetch the environment settings.
        let compressed_inputs = environment.compressed_inputs();
        let compressed_outputs = environment.compressed_outputs();
        let check_input_for_correctness = environment.check_input_for_correctness();

        trace!("Calculating previous contribution hash and writing it to the response");
        assert!(UseCompression::No == compressed_inputs, "Cannot hash a compressed file");
        let current_accumulator_hash = calculate_hash(reader);
        {
            debug!("`challenge` file contains decompressed points and has a hash:");
            Self::log_hash(&current_accumulator_hash);
            (&mut writer[0..])
                .write_all(current_accumulator_hash.as_slice())
                .expect("unable to write a challenge hash to mmap");
            writer.flush().expect("unable to write hash to response file");
        }
        {
            let mut challenge_hash = [0; 64];
            let mut memory_slice = reader.get(0..64).expect("must read point data from file");
            memory_slice
                .read_exact(&mut challenge_hash)
                .expect("couldn't read hash of challenge file from response file");
            debug!(
                "`challenge` file claims (!!! Must not be blindly trusted) it was based on the original contribution with a hash:"
            );
            Self::log_hash(&challenge_hash);
        }

        // Construct our keypair using the RNG we created above.
        let (public_key, private_key) =
            Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

        // Perform the transformation
        trace!("Computing and writing your contribution, this could take a while");
        Phase1::computation(
            reader,
            writer,
            compressed_inputs,
            compressed_outputs,
            check_input_for_correctness,
            &private_key,
            &parameters,
        )?;
        writer.flush()?;
        trace!("Finishing writing your contribution to response file");

        // Write the public key.
        public_key.write(writer, compressed_outputs, &parameters)?;

        Ok(())
    }

    /// Logs the hash.
    fn log_hash(hash: &[u8]) {
        let mut output = format!("\n\n");
        for line in hash.chunks(16) {
            output += "\t";
            for section in line.chunks(4) {
                for b in section {
                    output += &format!("{:02x}", b);
                }
                output += " ";
            }
            output += "\n";
        }
        debug!("{}", output);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Initialization},
        storage::Locator,
        testing::prelude::*,
    };

    use tracing::{debug, trace};

    #[test]
    #[serial]
    fn test_computation_run() {
        clear_test_transcript();

        // Define test parameters.
        let round_height = 0;
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();

        // Define test storage.
        let test_storage = test_storage();
        let mut storage = test_storage.write().unwrap();

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
