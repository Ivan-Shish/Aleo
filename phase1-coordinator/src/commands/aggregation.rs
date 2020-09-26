use crate::{environment::Environment, CoordinatorError};
use phase1::{helpers::CurveKind, Phase1Parameters};
use phase1_cli::combine;

use std::{panic, path::Path};
use zexe_algebra::{Bls12_377, BW6_761};

pub struct Aggregation;

impl Aggregation {
    /// Runs chunk aggregation for a given environment and round height.
    pub fn run(environment: &Environment, round_height: u64) -> anyhow::Result<()> {
        // Fetch the parameter settings.
        let settings = environment.to_settings();

        // Fetch the final transcript locator for the given round.
        let final_transcript_locator = environment.final_transcript_locator(round_height);

        // Check that the full transcript does not already exist.
        if Path::new(&final_transcript_locator).exists() {
            return Err(CoordinatorError::FullTranscriptAlreadyExists.into());
        }

        // Fetch the transcript directory for the given round.
        let transcript_directory = environment.transcript_directory(round_height);

        // Execute ceremony aggregation on chunk.
        let (_, _, curve, _, _, _) = settings.clone();
        let result = panic::catch_unwind(|| {
            match curve {
                CurveKind::Bls12_377 => combine(
                    &transcript_directory,
                    &final_transcript_locator,
                    &phase1_full_parameters!(Bls12_377, settings),
                ),
                CurveKind::BW6 => combine(
                    &transcript_directory,
                    &final_transcript_locator,
                    &phase1_full_parameters!(BW6_761, settings),
                ),
            };
        });

        match result.is_ok() {
            true => Ok(()),
            false => Err(CoordinatorError::ChunkAggregationFailed.into()),
        }
    }
}
