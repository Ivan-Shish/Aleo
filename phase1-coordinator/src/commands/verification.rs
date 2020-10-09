use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageWrite},
    CoordinatorError,
};
use phase1::helpers::CurveKind;
use phase1_cli::transform_pok_and_correctness;

use std::panic;
use tracing::{info, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub(crate) struct Verification;

impl Verification {
    ///
    /// Runs chunk verification for a given environment, round height, and chunk ID.
    ///
    /// Executes the round verification on a given chunk ID using phase1-cli logic.
    ///
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageWrite,
        round_height: u64,
        chunk_id: u64,
        current_contribution_id: u64,
        is_final_contribution: bool,
    ) -> anyhow::Result<()> {
        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && current_contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero.into());
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

        Self::execute(environment, storage, chunk_id, previous, current, next)?;

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );
        Ok(())
    }

    fn execute(
        environment: &Environment,
        storage: &mut StorageWrite,
        chunk_id: u64,
        previous: Locator,
        current: Locator,
        next: Locator,
    ) -> Result<(), CoordinatorError> {
        // Check that the previous and current locators exist in storage,
        // and fetch the readers for the previous and current locators.
        // let (previous, current) = match storage.exists(&previous) && storage.exists(&current) {
        //     true => (storage.reader(&previous)?, storage.reader(&current)?),
        //     false => return Err(CoordinatorError::StorageLocatorMissing),
        // };

        // Initialize the next contribution locator so the output is saved,
        // and fetch the writer for the next locator.
        storage.initialize(
            next.clone(),
            Object::contribution_file_size(environment, chunk_id, true),
        )?;
        // let next = match storage.initialize(&next) {
        //     Ok(_) => storage.writer(&next)?,
        //     Err(error) => return Err(error),
        // };

        // Fetch the compression settings.
        let compressed_input = environment.compressed_inputs();
        let compressed_output = environment.compressed_outputs();

        // Execute ceremony verification on chunk.
        let settings = environment.to_settings();
        let result = panic::catch_unwind(|| {
            let (_, _, curve, _, _, _) = settings.clone();
            match curve {
                CurveKind::Bls12_377 => transform_pok_and_correctness(
                    compressed_input,
                    &storage.to_path(&previous).unwrap(),
                    compressed_output,
                    &storage.to_path(&current).unwrap(),
                    compressed_input,
                    &storage.to_path(&next).unwrap(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => transform_pok_and_correctness(
                    compressed_input,
                    &storage.to_path(&previous).unwrap(),
                    compressed_output,
                    &storage.to_path(&current).unwrap(),
                    compressed_input,
                    &storage.to_path(&next).unwrap(),
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::VerificationFailed.into()),
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
