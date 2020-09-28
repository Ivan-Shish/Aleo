use crate::{environment::Environment, CoordinatorError};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters};
use phase1_cli::transform_pok_and_correctness;
use setup_utils::{blank_hash, calculate_hash, BatchSerializer, CheckForCorrectness, GenericArray, UseCompression};

use memmap::*;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    panic,
};
use tracing::{debug, error, info};
use typenum::consts::U64;
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Verification;

impl Verification {
    ///
    /// Runs chunk verification for a given environment, round height, and chunk ID.
    ///
    /// Executes the round verification on a given chunk ID using phase1-cli logic.
    ///
    pub fn run(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        previous_locator: String,
        current_locator: String,
        next_locator: String,
    ) -> anyhow::Result<()> {
        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero.into());

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
        }

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );

        // Execute ceremony verification on chunk.
        let (_, _, curve, _, _, _) = settings.clone();
        let result = panic::catch_unwind(|| {
            match curve {
                CurveKind::Bls12_377 => transform_pok_and_correctness(
                    &previous_locator,
                    &current_locator,
                    &next_locator,
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => transform_pok_and_correctness(
                    &previous_locator,
                    &current_locator,
                    &next_locator,
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, contribution_id
        );

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::VerificationFailed.into()),
        }
    }

    // pub fn copy_initial(
    //     environment: &Environment,
    //     round_height: u64,
    //     chunk_id: u64,
    //     contribution_hash: GenericArray<u8, U64>,
    // ) -> anyhow::Result<()> {
    //     // Fetch the parameter settings.
    //     let settings = environment.to_settings();
    //
    //     // Load the initial round contribution.
    //     let initial_round_contribution = environment.contribution_locator(round_height, chunk_id, 0);
    //     let initial_contribution_reader = OpenOptions::new()
    //         .read(true)
    //         .open(initial_round_contribution)
    //         .expect("unable to open initial round contribution");
    //     let initial_contribution_map = unsafe {
    //         MmapOptions::new()
    //             .map(&initial_contribution_reader)
    //             .expect("unable to create a map for initial round contribution")
    //     };
    //
    //     // Derive the expected file size of the contribution.
    //     let compressed = UseCompression::No;
    //     let (_, _, curve, _, _, _) = settings.clone();
    //     let expected = match curve {
    //         CurveKind::Bls12_377 => contribution_filesize!(Bls12_377, settings, chunk_id, compressed, true),
    //         CurveKind::BW6 => contribution_filesize!(BW6_761, settings, chunk_id, compressed, true),
    //     };
    //
    //     // Check the initial round contribution file size matches with expectation.
    //     {
    //         let metadata = initial_contribution_reader
    //             .metadata()
    //             .expect("unable to retrieve metadata");
    //         let found = metadata.len();
    //         debug!("Round {} contribution {} filesize is {}", round_height, chunk_id, found);
    //         if found != expected {
    //             error!("Contribution file size should be {} but found {}", expected, found);
    //             return Err(CoordinatorError::ContributionFileSizeMismatch.into());
    //         }
    //     }
    //
    //     // Load the new round contribution.
    //     let new_round_contribution = environment.contribution_locator(round_height + 1, chunk_id, 0);
    //     // Create new contribution file in this directory.
    //     let new_contribution_reader = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create_new(true)
    //         .open(new_round_contribution)
    //         .expect("unable to open new round contribution");
    //     // Set the length of the new contribution file.
    //     new_contribution_reader
    //         .set_len(expected)
    //         .expect("must make output file large enough");
    //     // Create the new contribution map.
    //     let mut new_contribution_map = unsafe {
    //         MmapOptions::new()
    //             .map_mut(&new_contribution_reader)
    //             .expect("unable to create new contribution map")
    //     };
    //     {
    //         (&mut new_contribution_map[0..])
    //             .write_all(contribution_hash.as_slice())
    //             .expect("unable to write a default hash to mmap");
    //         new_contribution_map
    //             .flush()
    //             .expect("unable to write hash to new challenge file");
    //     }
    //     // Copy the contribution files over.
    //     {
    //         let chunk_size = 1 << 30; // read by 1GB from map
    //         for chunk in new_contribution_map.chunks(chunk_size) {
    //             new_contribution_map.write_batch(&chunk, compressed);
    //         }
    //         new_contribution_map.flush().expect("must flush the memory map");
    //     }
    //     // Compute the new contribution hash.
    //     {
    //         let new_contribution_map = new_contribution_map.make_read_only().expect("must make a map readonly");
    //         let recompressed_hash = calculate_hash(&new_contribution_map);
    //
    //         debug!("Here's the BLAKE2b hash of the decompressed participant's response as new_challenge file:");
    //         Self::log_hash(&recompressed_hash, chunk_id);
    //         debug!("Done! new challenge file contains the new challenge file");
    //     }
    //
    //     Ok(())
    // }
    //
    // /// Logs the contribution hash.
    // fn log_hash(hash: &[u8], chunk_id: u64) {
    //     let mut output = format!("The contribution hash of chunk {} is:\n\n", chunk_id);
    //     for line in hash.chunks(16) {
    //         output += "\t";
    //         for section in line.chunks(4) {
    //             for b in section {
    //                 output += &format!("{:02x}", b);
    //             }
    //             output += " ";
    //         }
    //         output += "\n";
    //     }
    //     debug!("{}", output);
    // }
}
