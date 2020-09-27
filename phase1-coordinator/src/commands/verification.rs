use crate::{environment::Environment, CoordinatorError};
use phase1::{helpers::CurveKind, Phase1Parameters};
use phase1_cli::transform_pok_and_correctness;
use setup_utils::{blank_hash, calculate_hash};

use memmap::*;
use std::{fs::OpenOptions, panic};
use tracing::info;
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
    ) -> anyhow::Result<()> {
        // If there are no prior rounds, sanity check the initial contribution.
        if round_height == 0 && contribution_id == 0 {
            info!("Verifying the chunk contribution");

            // Open the transcript file.
            let transcript = environment.contribution_locator(round_height, chunk_id, contribution_id);
            let file = OpenOptions::new().read(true).open(&transcript)?;
            let reader = unsafe { MmapOptions::new().map(&file)? };

            // Check that the contribution chunk was generated based on the blank hash.
            let hash = blank_hash();
            for (expected, candidate) in hash.iter().zip(reader.chunks(64).next().unwrap()) {
                if expected != candidate {
                    return Err(CoordinatorError::ChunkVerificationFailed.into());
                }
            }

            // Compute the contribution hash to ensure it works.
            let _ = calculate_hash(&reader);

            info!("Completed verification of the chunk contribution");
            return Ok(());
        }

        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch the transcript locators.
        let previous_contribution = environment.contribution_locator(round_height, chunk_id, contribution_id - 1);
        let candidate_contribution = environment.contribution_locator(round_height, chunk_id, contribution_id);
        let next_contribution = environment.contribution_locator(round_height, chunk_id, contribution_id + 1);

        // Execute ceremony verification on chunk.
        let (_, _, curve, _, _, _) = settings.clone();
        let result = panic::catch_unwind(|| {
            match curve {
                CurveKind::Bls12_377 => transform_pok_and_correctness(
                    &previous_contribution,
                    &candidate_contribution,
                    &next_contribution,
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                ),
                CurveKind::BW6 => transform_pok_and_correctness(
                    &previous_contribution,
                    &candidate_contribution,
                    &next_contribution,
                    &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
                ),
            };
        });

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::ChunkVerificationFailed.into()),
        }
    }
}
