use crate::{
    environment::Environment,
    storage::{Locator, StorageWrite},
    CoordinatorError,
};
use phase1::helpers::CurveKind;
use phase1_cli::contribute;

use rand::thread_rng;
use std::{panic, time::Instant};
use tracing::{info, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub(crate) struct Computation;

impl Computation {
    ///
    /// Runs computation for a given environment, round height, chunk ID, and contribution ID.
    ///
    /// Executes the round computation on a given chunk ID and contribution ID using phase1-cli logic.
    ///
    pub(crate) fn run(
        environment: &Environment,
        storage: &StorageWrite,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> anyhow::Result<()> {
        info!(
            "Starting computation on round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );
        let now = Instant::now();

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch the contribution locators.
        let current_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id - 1, true);
        let next_locator = &Locator::ContributionFile(round_height, chunk_id, contribution_id, false);

        trace!(
            "Storing round {} chunk {} contribution {} in {}",
            round_height,
            chunk_id,
            contribution_id,
            storage.to_path(next_locator)?
        );

        // Execute computation on chunk.
        let compressed_input = environment.compressed_inputs();
        let challenge_path = &storage.to_path(current_locator)?;
        let compressed_output = environment.compressed_outputs();
        let response_path = &storage.to_path(next_locator)?;
        let check_input_for_correctness = environment.check_input_for_correctness();
        let result = panic::catch_unwind(|| {
            let (_, _, curve, _, _, _) = settings;
            match curve {
                CurveKind::Bls12_377 => contribute(
                    compressed_input,
                    challenge_path,
                    compressed_output,
                    response_path,
                    check_input_for_correctness,
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                    &mut thread_rng(),
                ),
                CurveKind::BW6 => contribute(
                    compressed_input,
                    challenge_path,
                    compressed_output,
                    response_path,
                    check_input_for_correctness,
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                    &mut thread_rng(),
                ),
            };
        });
        if result.is_err() {
            return Err(CoordinatorError::ComputationFailed.into());
        }

        let elapsed = Instant::now().duration_since(now);
        info!(
            "Completed computation on round {} chunk {} contribution {} in {:?}",
            round_height, chunk_id, contribution_id, elapsed
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Initialization},
        storage::{Locator, Storage},
        testing::prelude::*,
    };

    use memmap::MmapOptions;
    use std::{
        fs::OpenOptions,
        sync::{Arc, RwLock},
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
            Computation::run(&TEST_ENVIRONMENT_3, &storage, round_height, chunk_id, 1).unwrap();

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
