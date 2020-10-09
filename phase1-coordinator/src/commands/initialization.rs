use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageWrite},
    CoordinatorError,
};
use phase1::helpers::CurveKind;
use phase1_cli::new_challenge;
use setup_utils::calculate_hash;

use memmap::*;
use std::{fs, fs::OpenOptions, panic, time::Instant};
use tracing::{debug, info, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub(crate) struct Initialization;

impl Initialization {
    ///
    /// Runs chunk initialization for a given environment, round height, and chunk ID.
    ///
    /// Executes the round initialization on a given chunk ID using phase1-cli logic.
    ///
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageWrite,
        round_height: u64,
        chunk_id: u64,
    ) -> anyhow::Result<Vec<u8>> {
        info!("Starting initialization and migration on chunk {}", chunk_id);
        let now = Instant::now();

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Initialize and fetch a writer for the contribution locator so the output is saved.
        let contribution_locator = Locator::ContributionFile(round_height, chunk_id, 0, true);
        storage.initialize(
            contribution_locator.clone(),
            Object::contribution_file_size(environment, chunk_id),
        )?;
        // let contribution = storage.writer(&contribution_locator)?;

        // Execute ceremony initialization on chunk.
        let compressed_input = environment.compressed_inputs();
        let result = panic::catch_unwind(|| {
            let (_, _, curve, _, _, _) = settings;
            match curve {
                CurveKind::Bls12_377 => new_challenge(
                    compressed_input,
                    &storage.to_path(&contribution_locator).unwrap(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => new_challenge(
                    compressed_input,
                    &storage.to_path(&contribution_locator).unwrap(),
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });
        if result.is_err() {
            return Err(CoordinatorError::InitializationFailed.into());
        }

        trace!(
            "Storing round {} chunk {} in {}",
            round_height,
            chunk_id,
            storage.to_path(&contribution_locator)?
        );

        // Copy the current transcript to the next transcript.
        // This operation will *overwrite* the contents of `next_transcript`.
        let next_contribution_locator = Locator::ContributionFile(round_height + 1, chunk_id, 0, true);
        if let Err(error) = storage.copy(&contribution_locator, &next_contribution_locator) {
            return Err(error.into());
        }

        // Check that the current and next contribution hash match.
        let contribution_hash = Self::check(storage, &contribution_locator, &next_contribution_locator, chunk_id)?;

        info!(
            "Completed initialization and migration on chunk {} in {:?}",
            chunk_id,
            Instant::now().duration_since(now)
        );
        Ok(contribution_hash)
    }

    /// Compute both contribution hashes and check for equivalence.
    fn check(
        storage: &StorageWrite,
        contribution_locator: &Locator,
        next_contribution_locator: &Locator,
        chunk_id: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let current = storage.reader(contribution_locator)?;
        let next = storage.reader(next_contribution_locator)?;

        // Compare the contribution hashes of both files to ensure the copy succeeded.
        let contribution_hash_0 = calculate_hash(current.as_ref());
        let contribution_hash_1 = calculate_hash(next.as_ref());
        Self::log_hash(&contribution_hash_1, chunk_id);
        if contribution_hash_0 != contribution_hash_1 {
            return Err(CoordinatorError::InitializationTranscriptsDiffer.into());
        }

        Ok(contribution_hash_1.to_vec())
    }

    /// Logs the contribution hash.
    fn log_hash(hash: &[u8], chunk_id: u64) {
        let mut output = format!("The contribution hash of chunk {} is:\n\n", chunk_id);
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
        commands::Initialization,
        storage::{Locator, Storage},
        testing::prelude::*,
    };
    use setup_utils::{blank_hash, calculate_hash, GenericArray};

    use memmap::MmapOptions;
    use std::{
        fs::OpenOptions,
        sync::{Arc, RwLock},
    };
    use tracing::{debug, trace};

    #[test]
    #[serial]
    fn test_initialization_run() {
        clear_test_transcript();

        // Define test parameters.
        let round_height = 0;
        let number_of_chunks = TEST_ENVIRONMENT.number_of_chunks();

        // Define test storage.
        let test_storage = test_storage();
        let mut storage = test_storage.write().unwrap();

        // Initialize the previous contribution hash with a no-op value.
        let mut previous_contribution_hash: GenericArray<u8, _> =
            GenericArray::from_slice(vec![0; 64].as_slice()).clone();

        // Generate a new challenge for the given parameters.
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Execute the ceremony initialization
            let candidate_hash = Initialization::run(&TEST_ENVIRONMENT, &mut storage, round_height, chunk_id).unwrap();

            // Open the contribution locator file.
            let locator = Locator::ContributionFile(round_height, chunk_id, 0, true);
            let reader = storage.reader(&locator).unwrap();

            // Check that the contribution chunk was generated based on the blank hash.
            let hash = blank_hash();
            for (i, (expected, candidate)) in hash.iter().zip(reader.as_ref().chunks(64).next().unwrap()).enumerate() {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }

            // If chunk ID is under (number_of_chunks / 2), the contribution hash
            // of each iteration will match with Groth16 and Marlin.
            if chunk_id < (number_of_chunks / 2) as u64 {
                // Sanity only - Check that the current contribution hash matches the previous one.
                let contribution_hash = calculate_hash(reader.as_ref());
                assert_eq!(contribution_hash.to_vec(), candidate_hash);
                match chunk_id == 0 {
                    true => previous_contribution_hash = contribution_hash,
                    false => {
                        assert_eq!(previous_contribution_hash, contribution_hash);
                        previous_contribution_hash = contribution_hash;
                    }
                }
                Initialization::log_hash(&contribution_hash, chunk_id);
            }
        }
    }
}
