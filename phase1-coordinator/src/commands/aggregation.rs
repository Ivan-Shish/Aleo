use crate::{
    environment::Environment,
    objects::Round,
    storage::{Locator, Object, ObjectReader, StorageWrite},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1};

use tracing::{debug, error, trace};
use zexe_algebra::{Bls12_377, BW6_761};

pub(crate) struct Aggregation;

impl Aggregation {
    /// Runs aggregation for a given environment and round.
    #[inline]
    pub(crate) fn run(environment: &Environment, storage: &mut StorageWrite, round: &Round) -> anyhow::Result<()> {
        // Fetch the round height.
        let round_height = round.round_height();
        debug!("Starting aggregation on round {}", round_height);

        // Fetch the compressed input setting for the final round file.
        let compressed_input = environment.compressed_inputs();
        // Fetch the compressed output setting based on the round height.
        let compressed_output = environment.compressed_outputs();

        // Fetch the round locator for the given round.
        let round_locator = Locator::RoundFile(round_height);

        // Check that the round locator does not already exist.
        if storage.exists(&round_locator) {
            return Err(CoordinatorError::RoundLocatorAlreadyExists.into());
        }

        // Initialize the round locator.
        storage.initialize(
            round_locator.clone(),
            Object::round_file_size(environment, round_height),
        )?;

        // Load the contribution files.
        let readers = Self::readers(environment, storage, round)?;
        let contribution_readers: Vec<_> = readers.iter().map(|r| (r.as_ref(), compressed_output)).collect();

        // Execute aggregation on given round.
        let chunk_id = 0usize;
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings;
        let result = match curve {
            CurveKind::Bls12_377 => Phase1::aggregation(
                &contribution_readers,
                (storage.writer(&round_locator)?.as_mut(), compressed_input),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Phase1::aggregation(
                &contribution_readers,
                (storage.writer(&round_locator)?.as_mut(), compressed_input),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
            ),
        };

        if let Err(error) = result {
            error!("Aggregation failed with {}", error);
            Err(CoordinatorError::RoundAggregationFailed.into())
        } else {
            debug!("Completed aggregation on round {}", round_height);
            Ok(())
        }
    }

    /// Attempts to open every contribution for the given round and
    /// returns readers to each chunk contribution file.
    #[inline]
    fn readers<'a>(
        environment: &Environment,
        storage: &'a StorageWrite<'a>,
        round: &Round,
    ) -> anyhow::Result<Vec<ObjectReader<'a>>> {
        let mut readers = vec![];

        // Fetch the round height.
        let round_height = round.round_height();

        // Fetch the expected current contribution ID for each chunk in the given round.
        let expected_id = round.expected_number_of_contributions() - 1;

        for chunk_id in 0..environment.number_of_chunks() {
            trace!("Loading contribution for round {} chunk {}", round_height, chunk_id);

            // Fetch the contribution ID.
            let contribution_id = round.get_chunk(chunk_id)?.current_contribution_id();

            // Sanity check that all chunks have all contributions present.
            if expected_id != contribution_id {
                error!("Expects {} contributions, found {}", expected_id, contribution_id);
                return Err(CoordinatorError::NumberOfContributionsDiffer.into());
            }

            // Fetch the contribution locator.
            let contribution_locator = Locator::ContributionFile(round_height, chunk_id, contribution_id, false);
            trace!("Loading contribution from {}", storage.to_path(&contribution_locator)?);

            // Check the corresponding verified contribution locator exists.
            let verified_contribution = Locator::ContributionFile(round_height + 1, chunk_id, 0, true);
            if !storage.exists(&verified_contribution) {
                error!("{} is missing", storage.to_path(&verified_contribution)?);
                return Err(CoordinatorError::ContributionMissingVerifiedLocator.into());
            }

            // Fetch and save a reader for the contribution locator.
            readers.push(storage.reader(&contribution_locator)?);

            trace!("Loaded contribution for round {} chunk {}", round_height, chunk_id);
        }

        Ok(readers)
    }
}

#[cfg(test)]
mod tests {
    use crate::{commands::Aggregation, testing::prelude::*, Coordinator};

    use once_cell::sync::Lazy;

    #[test]
    #[serial]
    fn test_aggregation_run() {
        initialize_test_environment();

        let coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone()).unwrap();
        let test_storage = coordinator.storage();

        // Ensure the ceremony has not started.
        assert_eq!(0, coordinator.current_round_height().unwrap());

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();

        // Run initialization.
        coordinator
            .next_round(*TEST_STARTED_AT, vec![contributor.clone()], vec![verifier.clone()])
            .unwrap();

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height().unwrap());

        // Define test parameters.
        let round_height = coordinator.current_round_height().unwrap();
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();

        // Iterate over all chunk IDs.
        for chunk_id in 0..number_of_chunks {
            {
                // Acquire the lock as contributor.
                let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor.clone());
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    try_lock.unwrap();
                }

                // Run computation as contributor.
                let contribute = coordinator.run_computation(round_height, chunk_id, 1, &contributor.clone());
                if contribute.is_err() {
                    println!(
                        "Failed to run computation as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    contribute.unwrap();
                }

                // Add the contribution as the contributor.
                let contribute = coordinator.add_contribution(chunk_id, &contributor.clone());
                if contribute.is_err() {
                    println!(
                        "Failed to add contribution as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    contribute.unwrap();
                }
            }
            {
                // Acquire the lock as the verifier.
                let try_lock = coordinator.try_lock_chunk(chunk_id, &verifier.clone());
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock as verifier {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    try_lock.unwrap();
                }

                // Run verification as the verifier.
                let verify = coordinator.verify_contribution(chunk_id, &verifier.clone());
                if verify.is_err() {
                    println!(
                        "Failed to run verification as verifier {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    verify.unwrap();
                }
            }
        }

        // Fetch the current round state.
        let round = coordinator.get_round(round_height).unwrap();

        // Aggregate.
        {
            // Obtain the storage lock.
            let mut storage = test_storage.write().unwrap();

            // Run aggregation on the round.
            Aggregation::run(&TEST_ENVIRONMENT_3, &mut storage, &round).unwrap();
        }
    }
}
