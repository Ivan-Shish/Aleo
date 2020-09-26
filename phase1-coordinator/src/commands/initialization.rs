use crate::{
    environment::{Environment, Settings},
    storage::{Key, Storage, Value},
};
use phase1::{helpers::CurveKind, CurveParameters, Phase1, Phase1Parameters};
use phase1_cli::new_challenge;
use setup_utils::{blank_hash, calculate_hash, print_hash, UseCompression};

use chrono::{DateTime, TimeZone, Utc};
use memmap::*;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    time::Instant,
};
use tracing::{debug, info};
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Initialization;

impl Initialization {
    /// Runs ceremony initialization for the given environment.
    pub fn run(environment: &Environment) -> anyhow::Result<()> {
        info!("Starting ceremony initialization");

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch the number of chunks.
        let num_chunks = environment.number_of_chunks();

        // Create the local `transcript` directory and a new `initialization-{hash}` subdirectory.
        let transcript_directory = {
            let timestamp = Utc::now().format("%Y-%m-%d_%H.%M.%S").to_string();
            let transcript_directory = format!("./transcript/initialization_{}", timestamp);
            fs::create_dir_all(&transcript_directory)?;
        };

        // Generate a new challenge for the given parameters.
        for chunk_id in 0..num_chunks {
            info!("Starting initialization on chunk {}", chunk_id);
            let now = Instant::now();

            // Create a transcript file located at `{transcript_directory}/{chunk_id}`.
            let transcript = format!("{:?}/{}", transcript_directory, chunk_id);

            Self::execute_chunk(settings.clone(), chunk_id, &transcript);

            let elapsed = Instant::now().duration_since(now);
            info!("Completed initialization on chunk {} in {:?}", chunk_id, elapsed);
        }

        info!("Completed ceremony initialization");
        Ok(())
    }

    /// Executes the ceremony initialization on a given chunk ID using phase1-cli logic.
    fn execute_chunk(settings: Settings, chunk_id: u64, transcript: &str) -> anyhow::Result<()> {
        // Execute ceremony initialization on chunk.
        let (_, _, curve, _, _, _) = settings;
        match curve {
            CurveKind::Bls12_377 => new_challenge(transcript, &phase1_parameters!(Bls12_377, settings, chunk_id)),
            CurveKind::BW6 => new_challenge(transcript, &phase1_parameters!(BW6_761, settings, chunk_id)),
        };

        // Open the transcript file.
        let file = OpenOptions::new().read(true).open(transcript)?;
        let reader = unsafe { MmapOptions::new().map(&file)? };

        // Verify the contribution hash.
        let contribution_hash = calculate_hash(&reader);
        Self::log_hash(&contribution_hash, chunk_id);

        Ok(())
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
    use crate::{environment::Parameters, testing::prelude::*};
    use phase1::{Phase1, ProvingSystem};
    use setup_utils::{CheckForCorrectness, GenericArray};

    const NUM_CHUNKS: u64 = 10;

    #[test]
    fn test_initialization_execute_chunk() {
        test_logger();

        // Create the local testing `transcript` directory and a new `initialization-{hash}` subdirectory.
        let timestamp = Utc::now().format("%Y-%m-%d_%H.%M.%S").to_string();
        let transcript_directory = format!("./transcript/testing/initialization_{}", timestamp);
        fs::create_dir_all(&transcript_directory).unwrap();

        let settings = TEST_ENVIRONMENT.to_settings();

        let mut previous_contribution_hash: GenericArray<u8, _> =
            GenericArray::from_slice(vec![0; 64].as_slice()).clone();

        // Generate a new challenge for the given parameters.
        let (_, _, curve, _, _, _) = &settings;
        for chunk_id in 0..NUM_CHUNKS {
            // Create a transcript file located at `{transcript_directory}/{chunk_id}`.
            let transcript = format!("{}/{}", transcript_directory, chunk_id);
            println!("{:?}", transcript);

            // Execute the ceremony initialization
            Initialization::execute_chunk(settings.clone(), chunk_id, &transcript);

            // Open the transcript file.
            let transcript = format!("{}/{}", transcript_directory, chunk_id);
            let file = OpenOptions::new().read(true).open(transcript).unwrap();
            let reader = unsafe { MmapOptions::new().map(&file).unwrap() };

            // Load the output transcript into a buffer.
            let chunk_size = 1 << 30; // read by 1GB from map
            let mut buffer = reader.chunks(chunk_size);

            // Check that the contribution chunk was generated based on the blank hash.
            let hash = blank_hash();
            for (expected, candidate) in hash.iter().zip(buffer.next().unwrap()) {
                assert_eq!(expected, candidate);
            }

            // Sanity only - Check that the current contribution hash matches the previous one.
            let contribution_hash = calculate_hash(&reader);
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
