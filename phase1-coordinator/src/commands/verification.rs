use crate::{environment::Environment, storage::StorageWriter, CoordinatorError};
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
        storage: &mut StorageWriter,
        round_height: u64,
        chunk_id: u64,
        current_contribution_id: u64,
        is_final_contribution: bool,
    ) -> anyhow::Result<()> {
        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && current_contribution_id == 0 {
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
            round_height, chunk_id, current_contribution_id
        );

        // Fetch the contribution locators for `Verification`.
        let (previous, current, next) = if is_final_contribution {
            let previous = storage.contribution_locator(round_height, chunk_id, current_contribution_id - 1, true);
            let current = storage.contribution_locator(round_height, chunk_id, current_contribution_id, false);
            let next = storage.contribution_locator(round_height + 1, chunk_id, 0, true);

            // Initialize the next contribution locator of the new round so the file will be saved.
            storage.contribution_locator_init(round_height + 1, chunk_id, 0, true);

            (previous, current, next)
        } else {
            let previous = storage.contribution_locator(round_height, chunk_id, current_contribution_id - 1, true);
            let current = storage.contribution_locator(round_height, chunk_id, current_contribution_id, false);
            let next = storage.contribution_locator(round_height, chunk_id, current_contribution_id, true);
            (previous, current, next)
        };

        trace!("Previous contribution locator is {}", previous);
        trace!("Current contribution locator is {}", current);
        trace!("Next contribution locator is {}", next);

        let compressed_input = environment.compressed_inputs();
        let compressed_output = environment.compressed_outputs();

        // Execute ceremony verification on chunk.
        let (_, _, curve, _, _, _) = settings.clone();
        let result = panic::catch_unwind(|| {
            match curve {
                CurveKind::Bls12_377 => transform_pok_and_correctness(
                    compressed_input,
                    &previous,
                    compressed_output,
                    &current,
                    compressed_input,
                    &next,
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => transform_pok_and_correctness(
                    compressed_input,
                    &previous,
                    compressed_output,
                    &current,
                    compressed_input,
                    &next,
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::VerificationFailed.into()),
        }
    }
}
