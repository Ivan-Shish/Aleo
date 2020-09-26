use crate::environment::Environment;
use phase1::{helpers::CurveKind, Phase1Parameters};
use phase1_cli::new_challenge;
use setup_utils::calculate_hash;

use memmap::*;
use std::{fs::OpenOptions, time::Instant};
use tracing::{debug, info, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Initialization;

impl Initialization {
    ///
    /// Runs chunk initialization for a given environment, round height, and chunk ID.
    ///
    /// Executes the round initialization on a given chunk ID using phase1-cli logic.
    ///
    pub fn run(environment: &Environment, round_height: u64, chunk_id: u64) -> anyhow::Result<Vec<u8>> {
        info!("Starting initialization on chunk {}", chunk_id);
        let now = Instant::now();

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch the transcript locator.
        let transcript = environment.transcript_locator(round_height, chunk_id);
        trace!("Storing round {} chunk {} in {}", round_height, chunk_id, transcript);

        // Execute ceremony initialization on chunk.
        let (_, _, curve, _, _, _) = settings;
        match curve {
            CurveKind::Bls12_377 => new_challenge(&transcript, &phase1_parameters!(Bls12_377, settings, chunk_id)),
            CurveKind::BW6 => new_challenge(&transcript, &phase1_parameters!(BW6_761, settings, chunk_id)),
        };

        // Open the transcript file.
        let file = OpenOptions::new().read(true).open(&transcript)?;
        let reader = unsafe { MmapOptions::new().map(&file)? };

        // Compute the contribution hash.
        let contribution_hash = calculate_hash(&reader);
        Self::log_hash(&contribution_hash, chunk_id);

        let elapsed = Instant::now().duration_since(now);
        info!("Completed initialization on chunk {} in {:?}", chunk_id, elapsed);
        Ok(contribution_hash.to_vec())
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
    use super::*;
    use crate::testing::prelude::*;
    use setup_utils::{blank_hash, GenericArray};

    use tracing::trace;

    #[test]
    #[serial]
    fn test_initialization_run() {
        test_logger();
        clear_test_transcript();

        // Define test parameters.
        let round_height = 0;
        let number_of_chunks = TEST_ENVIRONMENT.number_of_chunks();

        // Initialize the previous contribution hash with a no-op value.
        let mut previous_contribution_hash: GenericArray<u8, _> =
            GenericArray::from_slice(vec![0; 64].as_slice()).clone();

        // Generate a new challenge for the given parameters.
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Execute the ceremony initialization
            let candidate_hash = Initialization::run(&TEST_ENVIRONMENT, round_height, chunk_id).unwrap();

            // Open the transcript locator file.
            let transcript = TEST_ENVIRONMENT.transcript_locator(round_height, chunk_id);
            let file = OpenOptions::new().read(true).open(transcript).unwrap();
            let reader = unsafe { MmapOptions::new().map(&file).unwrap() };

            // Check that the contribution chunk was generated based on the blank hash.
            let hash = blank_hash();
            for (i, (expected, candidate)) in hash.iter().zip(reader.chunks(64).next().unwrap()).enumerate() {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }

            // If chunk ID is under (number_of_chunks / 2), the contribution hash
            // of each iteration will match with Groth16 and Marlin.
            if chunk_id < (number_of_chunks / 2) as u64 {
                // Sanity only - Check that the current contribution hash matches the previous one.
                let contribution_hash = calculate_hash(&reader);
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
