use crate::{
    environment::Environment,
    objects::Round,
    storage::{Locator, Object, ObjectReader, ObjectWriter, StorageWrite},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1};
use setup_utils::UseCompression;

use memmap::*;
use std::fs::OpenOptions;
use tracing::{debug, error, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub(crate) struct Aggregation;

impl Aggregation {
    /// Runs aggregation for a given environment and round.
    pub(crate) fn run(environment: &Environment, storage: &StorageWrite, round: &Round) -> anyhow::Result<()> {
        // Fetch the round height.
        let round_height = round.round_height();
        // Fetch the compressed input setting for the final round file.
        let compressed_input = environment.compressed_inputs();
        // Fetch the compressed output setting based on the round height.
        let compressed_output = environment.compressed_outputs();

        // Load the contribution files.
        let readers = Self::readers(environment, storage, round)?;
        let contribution_readers: Vec<_> = readers.iter().map(|r| (r.as_ref(), compressed_output)).collect();

        // Load the final round file.
        // let round_writer = ;

        debug!("Starting aggregation on round {}", round_height);

        // Execute aggregation on given round.
        let chunk_id = 0usize;
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        let result = match curve {
            CurveKind::Bls12_377 => Phase1::aggregation(
                &contribution_readers,
                (Self::writer(storage, round)?.as_mut(), compressed_input),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Phase1::aggregation(
                &contribution_readers,
                (Self::writer(storage, round)?.as_mut(), compressed_input),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
            ),
        };

        if let Err(error) = result {
            error!("Aggregation failed during execution ({})", error);
            Err(CoordinatorError::RoundAggregationFailed.into())
        } else {
            debug!("Completed aggregation on round {}", round_height);
            Ok(())
        }
    }

    /// Attempts to open every contribution for the given round and
    /// returns readers to each chunk contribution file.
    fn readers<'a>(
        environment: &Environment,
        storage: &'a StorageWrite<'a>,
        round: &Round,
    ) -> anyhow::Result<Vec<ObjectReader<'a>>>
// where
    //     dyn ObjectReader<'a>: Sized,
    {
        let mut readers = vec![];

        // Fetch the round height.
        let round_height = round.round_height();

        // Fetch the final contribution ID for each chunk in the given round.
        let final_contribution_id = round.expected_number_of_contributions() - 1;

        for chunk_id in 0..environment.number_of_chunks() {
            trace!("Loading contribution from chunk {}", chunk_id);

            // Fetch the contribution ID.
            let current_contribution_id = round.get_chunk(chunk_id)?.current_contribution_id();

            // Sanity check that each contribution ID is the same,
            // meaning all chunks have the same number of contributions
            // contributed to it.
            if current_contribution_id != final_contribution_id {
                return Err(CoordinatorError::NumberOfContributionsDiffer.into());
            }

            // Fetch the reader with the contribution locator.
            let contribution_locator =
                Locator::ContributionFile(round_height, chunk_id, current_contribution_id, false);
            let reader = storage.reader(&contribution_locator)?;

            readers.push(reader);
        }

        Ok(readers)
    }

    /// Attempts to create the contribution file for the given round and
    /// returns a writer to it.
    fn writer<'a>(storage: &'a StorageWrite<'a>, round: &Round) -> anyhow::Result<ObjectWriter<'a>> {
        // Fetch the round height.
        let round_height = round.round_height();

        // Fetch the round locator for the given round.
        let round_locator = Locator::RoundFile(round_height);

        // Check that the round locator does not already exist.
        if storage.exists(&round_locator) {
            return Err(CoordinatorError::RoundLocatorAlreadyExists.into());
        }

        // Create the writer for the round locator.
        Ok(storage.writer(&round_locator)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::{Computation, Verification},
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;
    use tracing::debug;

    fn initialize_coordinator(coordinator: &Coordinator) -> anyhow::Result<()> {
        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height()?);

        // Run initialization.
        coordinator.next_round(*TEST_STARTED_AT, vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()], vec![
            Lazy::force(&TEST_VERIFIER_ID).clone(),
        ])?;

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height()?);
        Ok(())
    }

    // #[test]
    // #[serial]
    // fn test_aggregation_run() {
    //     clear_test_transcript();
    //
    //     // Initialize round 0 and 1.
    //     let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone()).unwrap();
    //     initialize_coordinator(&coordinator).unwrap();
    //
    //     // Generate a new contribution for each chunk in round 1.
    //     let round_height = 1;
    //     let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();
    //
    //     // let mut locators = vec![];
    //
    //     // Run computation for all chunks in round 1.
    //     for chunk_id in 0..number_of_chunks {
    //         debug!("Running computation on test chunk {}", chunk_id);
    //         // Run computation on chunk.
    //         Computation::run(&TEST_ENVIRONMENT_3, storage, round_height, chunk_id, 1).unwrap();
    //
    //         let previous = TEST_ENVIRONMENT_3.contribution_locator(round_height, chunk_id, 0, true);
    //         let current = TEST_ENVIRONMENT_3.contribution_locator(round_height, chunk_id, 1, false);
    //         let next = TEST_ENVIRONMENT_3.contribution_locator(round_height, chunk_id, 2, true);
    //
    //         Verification::run(&TEST_ENVIRONMENT_3, round_height, chunk_id, 1, previous, current, next).unwrap();
    //     }
    //
    //     // TODO (howardwu): Update and finish this test to reflect new compressed output setting.
    //
    //     // Aggregation::run(&TEST_ENVIRONMENT_3, &coordinator.get_round(round_height).unwrap()).unwrap();
    // }
}
