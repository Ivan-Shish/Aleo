use crate::{
    coordinator_state::Task,
    environment::{Parameters, Testing},
    testing::prelude::*,
    Coordinator,
    Participant,
};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{collections::HashSet, panic};

fn create_contributor(id: &str) -> Participant {
    Participant::Contributor(format!("test-contributor-{}", id))
}

fn create_verifier(id: &str) -> Participant {
    Participant::Verifier(format!("test-verifier-{}", id))
}

fn execute_round_test(proving_system: ProvingSystem, curve: CurveKind) -> anyhow::Result<()> {
    let parameters = Parameters::Custom((
        ContributionMode::Chunked,
        proving_system,
        curve,
        7,  /* power */
        32, /* batch_size */
        32, /* chunk_size */
    ));
    let environment = initialize_test_environment_with_debug(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let coordinator = Coordinator::new(environment)?;

    // Initialize the ceremony to round 1.
    assert_eq!(0, coordinator.current_round_height()?);
    coordinator.initialize()?;
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let contributor = create_contributor("1");
    let verifier = create_verifier("1");
    coordinator.add_to_queue(contributor.clone(), 10)?;
    coordinator.add_to_queue(verifier.clone(), 10)?;
    assert_eq!(1, coordinator.number_of_queue_contributors());
    assert_eq!(1, coordinator.number_of_queue_verifiers());

    // Update the ceremony to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..number_of_chunks {
        coordinator.contribute(&contributor)?;
        coordinator.verify(&verifier)?;
    }

    //
    // Add a contributor and verifier to the queue.
    //
    // Note: This logic for adding to the queue works because
    // `Environment::allow_current_contributors_in_queue`
    // and `Environment::allow_current_verifiers_in_queue`
    // are set to `true`. This section can be removed without
    // changing the outcome of this test, if necessary.
    //
    let contributor = create_contributor("1");
    let verifier = create_verifier("1");
    coordinator.add_to_queue(contributor.clone(), 10)?;
    coordinator.add_to_queue(verifier.clone(), 10)?;
    assert_eq!(1, coordinator.number_of_queue_contributors());
    assert_eq!(1, coordinator.number_of_queue_verifiers());

    // Update the ceremony to round 3.
    coordinator.update()?;
    assert_eq!(3, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());

    Ok(())
}

/// Drops a contributor who does not affect other contributors or verifiers.
fn coordinator_drop_contributor_basic_test() -> anyhow::Result<()> {
    let parameters = Parameters::Custom((
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment_with_debug(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let coordinator = Coordinator::new(environment)?;

    // Initialize the ceremony to round 1.
    assert_eq!(0, coordinator.current_round_height()?);
    coordinator.initialize()?;
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let contributor1 = create_contributor("1");
    let contributor2 = create_contributor("2");
    let verifier = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), 10)?;
    coordinator.add_to_queue(contributor2.clone(), 9)?;
    coordinator.add_to_queue(verifier.clone(), 10)?;
    assert_eq!(2, coordinator.number_of_queue_contributors());
    assert_eq!(1, coordinator.number_of_queue_verifiers());
    assert!(coordinator.is_queue_contributor(&contributor1));
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.is_queue_verifier(&verifier));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Update the ceremony to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..(number_of_chunks - 1) {
        coordinator.contribute(&contributor1)?;
        coordinator.contribute(&contributor2)?;
        coordinator.verify(&verifier)?;
        coordinator.verify(&verifier)?;
    }
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Drop the contributor from the current round.
    let locators = coordinator.drop_participant(&contributor1)?;
    assert_eq!(&number_of_chunks - 1, locators.len());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Check that contributor 1 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(2, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor1).count());
    for (contributor, contributor_info) in contributors {
        if contributor == contributor2 {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(7, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Print the coordinator state.
    let coordinator_state = coordinator.state();
    let state = coordinator_state.read().unwrap();
    debug!("{}", serde_json::to_string_pretty(&*state)?);
    assert_eq!(2, state.current_round_height());

    Ok(())
}

/// Drops a contributor in between two contributors.
fn coordinator_drop_contributor_in_between_two_contributors_test() -> anyhow::Result<()> {
    let parameters = Parameters::Custom((
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment_with_debug(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let coordinator = Coordinator::new(environment.clone())?;

    // Initialize the ceremony to round 1.
    assert_eq!(0, coordinator.current_round_height()?);
    coordinator.initialize()?;
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let contributor1 = create_contributor("1");
    let contributor2 = create_contributor("2");
    let contributor3 = create_contributor("3");
    let verifier = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), 10)?;
    coordinator.add_to_queue(contributor2.clone(), 9)?;
    coordinator.add_to_queue(contributor3.clone(), 8)?;
    coordinator.add_to_queue(verifier.clone(), 10)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());
    assert_eq!(1, coordinator.number_of_queue_verifiers());

    // Update the ceremony to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..(number_of_chunks - 1) {
        coordinator.contribute(&contributor1)?;
        coordinator.contribute(&contributor2)?;
        coordinator.contribute(&contributor3)?;
        coordinator.verify(&verifier)?;
        coordinator.verify(&verifier)?;
        coordinator.verify(&verifier)?;
    }
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Drop the contributor from the current round.
    let locators = coordinator.drop_participant(&contributor2)?;
    assert_eq!(&number_of_chunks - 1, locators.len());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Print the coordinator state.
    let coordinator_state = coordinator.state();
    let state = coordinator_state.read().unwrap();
    debug!("{}", serde_json::to_string_pretty(&*state)?);
    assert_eq!(2, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(8, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(7, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

/// Drops a contributor with other contributors in pending tasks.
fn coordinator_drop_contributor_with_contributors_in_pending_tasks_test() -> anyhow::Result<()> {
    let parameters = Parameters::Custom((
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment_with_debug(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let coordinator = Coordinator::new(environment.clone())?;

    // Initialize the ceremony to round 1.
    assert_eq!(0, coordinator.current_round_height()?);
    coordinator.initialize()?;
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let contributor1 = create_contributor("1");
    let contributor2 = create_contributor("2");
    let contributor3 = create_contributor("3");
    let verifier = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), 10)?;
    coordinator.add_to_queue(contributor2.clone(), 9)?;
    coordinator.add_to_queue(contributor3.clone(), 8)?;
    coordinator.add_to_queue(verifier.clone(), 10)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());
    assert_eq!(1, coordinator.number_of_queue_verifiers());

    // Update the ceremony to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());

    // Contribute and verify up to 2 before the final chunk.
    for _ in 0..(number_of_chunks - 2) {
        coordinator.contribute(&contributor1)?;
        coordinator.contribute(&contributor2)?;
        coordinator.contribute(&contributor3)?;
        coordinator.verify(&verifier)?;
        coordinator.verify(&verifier)?;
        coordinator.verify(&verifier)?;
    }

    // Lock the next task for contributor 1 and 3.
    coordinator.try_lock(&contributor1)?;
    coordinator.try_lock(&contributor3)?;

    // Check that coordinator state includes a pending task for contributor 1 and 3.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(1, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 || contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(1, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Drop the contributor from the current round.
    let locators = coordinator.drop_participant(&contributor2)?;
    assert_eq!(&number_of_chunks - 2, locators.len());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_queue_verifier(&verifier));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));
    assert!(!coordinator.is_finished_verifier(&verifier));

    // Print the coordinator state.
    let coordinator_state = coordinator.state();
    let state = coordinator_state.read().unwrap();
    debug!("{}", serde_json::to_string_pretty(&*state)?);
    assert_eq!(2, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(1, contributor_info.disposing_tasks().len());
            assert_eq!(7, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(1, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

#[test]
#[serial]
fn test_round_on_groth16_bls12_377() {
    execute_round_test(ProvingSystem::Groth16, CurveKind::Bls12_377).unwrap();
}

#[test]
#[serial]
fn test_round_on_groth16_bw6_761() {
    execute_round_test(ProvingSystem::Groth16, CurveKind::BW6).unwrap();
}

#[test]
#[serial]
fn test_round_on_marlin_bls12_377() {
    execute_round_test(ProvingSystem::Marlin, CurveKind::Bls12_377).unwrap();
}

#[test]
#[named]
#[serial]
fn test_coordinator_drop_contributor_basic() {
    test_report!(coordinator_drop_contributor_basic_test);
}

#[test]
#[named]
#[serial]
fn test_coordinator_drop_contributor_in_between_two_contributors() {
    test_report!(coordinator_drop_contributor_in_between_two_contributors_test);
}

#[test]
#[named]
#[serial]
fn test_coordinator_drop_contributor_with_contributors_in_pending_tasks() {
    test_report!(coordinator_drop_contributor_with_contributors_in_pending_tasks_test);
}
