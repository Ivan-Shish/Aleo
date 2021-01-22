use crate::{
    authentication::Dummy,
    commands::Seed,
    environment::{Parameters, Testing},
    testing::initialize_test_environment_with_debug,
    Coordinator,
    Participant,
};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};

use anyhow::Context;
use proptest::{
    prelude::{any, ProptestConfig},
    strategy::Strategy,
};
use proptest_derive::Arbitrary;
use tracing::debug;

use std::{collections::HashMap, panic};

use super::{create_contributor, create_verifier};

/// Info about a [Particpant] (verifier or contributor) to be created
/// and used in a proptest.
#[derive(Debug, Arbitrary, Clone)]
struct ParticipantProptestParams {
    reliability_score: u8,
    /// Which round of the ceremony this participant will join in.
    #[proptest(strategy = "0..3u64")]
    join_round: u64,
}

/// Info about a contributor being used in a proptest.
struct ContributorProptestInfo {
    /// Parameters used to create this contributor.
    params: ParticipantProptestParams,
    signing_key: String,
    seed: Seed,
}

/// Info about a verifier being used in a proptest.
struct VerifierProptestInfo {
    /// Parameters used to create this verifier.
    params: ParticipantProptestParams,
    signing_key: String,
}

proptest::prop_compose! {
    /// Generate [Parameters::Custom] to use in a [proptest].
    fn parameters_custom_strategy()(
        // batch sizes <= 1 are not supported
        batch_size in 2..16usize,
        chunk_size in 1..16usize
    ) -> Parameters {
        Parameters::Custom((
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            6,  /* power */
            batch_size,
            chunk_size,
        ))
    }
}

/// Returns true if the list of participant `params` contains a
/// participant who will enter the ceremony in round 0.
fn contains_round0_participant(params: &[ParticipantProptestParams]) -> bool {
    params.iter().find(|param| param.join_round == 0).is_some()
}

/// Returns `true` if the list of participant `params` contains a
/// participant who will enter the ceremony after the specified
/// `round`.
fn contains_participant_above_round(params: &[ParticipantProptestParams], round: u64) -> bool {
    params.iter().find(|param| param.join_round > round).is_some()
}

/// [Strategy] for generating vector of contributor [Participant]s for
/// use in a proptest. Generates between 1 and 10 contributors.
fn contributors_strategy() -> impl Strategy<Value = Vec<ParticipantProptestParams>> {
    proptest::collection::vec(any::<ParticipantProptestParams>(), 1..=10).prop_filter(
        "`contributors` must contain at least one contributor joining the ceremony in round 0",
        |contributors| contains_round0_participant(contributors),
    )
}

/// [Strategy] for generating vector of verifier [Participant]s for
/// use in a proptest. Generates between 1 and 10 verifiers.
fn verifiers_stragety() -> impl Strategy<Value = Vec<ParticipantProptestParams>> {
    proptest::collection::vec(any::<ParticipantProptestParams>(), 1..=10).prop_filter(
        "`verifiers` must contain at least one verifier joining the ceremony in round 0",
        |contributors| contains_round0_participant(contributors),
    )
}

proptest::proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    /// This test has a low number of cases because it takes so long
    /// to run, so it may fail intermittently if there is a bug.
    /// **Please do not ignore spurious failures in CI or your
    /// personal testing**. If there is a failure [it should produce a
    /// persistence file](https://altsysrq.github.io/proptest-book/proptest/failure-persistence.html)
    /// that you should consider checking into source control.
    #[test]
    fn coordinator_proptest(
        parameters in parameters_custom_strategy(),
        rounds in 1..3u64,
        contributor_params in contributors_strategy(),
        verifier_params in verifiers_stragety(),
    ) {
        proptest::prop_assume!(
            !contains_participant_above_round(&contributor_params, rounds),
            "`contributors` must not contain any contributor joining the \
            ceremony after the specified number of rounds ({})",
            rounds
        );
        proptest::prop_assume!(
            !contains_participant_above_round(&verifier_params, rounds),
            "`verifiers` must not contain any contributor joining the \
            ceremony after the specified number of rounds ({})",
            rounds
        );
        coordinator_proptest_impl(
            parameters,
            rounds,
            contributor_params,
            verifier_params)
            .context("Error during proptest")
            .unwrap();
    }
}

fn proptest_add_contributor(
    coordinator: &Coordinator,
    contributor: Participant,
    proptest_info: &ContributorProptestInfo,
) -> anyhow::Result<()> {
    coordinator
        .add_to_queue(contributor.clone(), proptest_info.params.reliability_score)
        .map_err(anyhow::Error::from)
        .context("error while adding contributor to coordinator")?;
    assert!(coordinator.is_queue_contributor(&contributor));
    assert!(!coordinator.is_current_contributor(&contributor));
    assert!(!coordinator.is_finished_contributor(&contributor));
    Ok(())
}

fn proptest_add_verifier(
    coordinator: &Coordinator,
    verifier: Participant,
    proptest_info: &VerifierProptestInfo,
) -> anyhow::Result<()> {
    coordinator
        .add_to_queue(verifier.clone(), proptest_info.params.reliability_score)
        .map_err(anyhow::Error::from)
        .context("error while adding verifier to coordinator")?;
    assert!(coordinator.is_queue_verifier(&verifier));
    assert!(!coordinator.is_current_verifier(&verifier));
    assert!(!coordinator.is_finished_verifier(&verifier));
    Ok(())
}

// Implementation is in a seperate function so VSCode gives proper
// syntax highlighting/metadata.
fn coordinator_proptest_impl(
    parameters: Parameters,
    rounds: u64,
    contributor_params: Vec<ParticipantProptestParams>,
    verifier_params: Vec<ParticipantProptestParams>,
) -> anyhow::Result<()> {
    // Create a new test environment.
    let environment = initialize_test_environment_with_debug(&Testing::from(parameters).into());

    // Instantiate a coordinator.
    let coordinator = Coordinator::new(environment, Box::new(Dummy))
        .map_err(anyhow::Error::from)
        .context("error while creating coordinator")?;

    // Initialize the ceremony to round 0.
    coordinator
        .initialize()
        .map_err(anyhow::Error::from)
        .context("error initializing coordinator")?;

    assert_eq!(
        0,
        coordinator
            .current_round_height()
            .map_err(anyhow::Error::from)
            .context("error obtaining current round height")?
    );

    let contributors: HashMap<Participant, ContributorProptestInfo> = contributor_params
        .iter()
        .filter(|params| params.join_round == 0)
        .enumerate()
        .map(|(i, params)| {
            let (contributor, signing_key, seed) = create_contributor(&i.to_string());

            let info = ContributorProptestInfo {
                params: params.clone(),
                signing_key,
                seed,
            };

            (contributor, info)
        })
        .collect();

    let verifiers: HashMap<Participant, VerifierProptestInfo> = verifier_params
        .iter()
        .filter(|params| params.join_round == 0)
        .enumerate()
        .map(|(i, params)| {
            let (verifier, signing_key) = create_verifier(&i.to_string());

            let info = VerifierProptestInfo {
                params: params.clone(),
                signing_key,
            };

            (verifier, info)
        })
        .collect();

    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert_eq!(0, coordinator.number_of_queue_verifiers());

    for (contributor, proptest_info) in &contributors {
        proptest_add_contributor(&coordinator, contributor.clone(), proptest_info)?;
    }

    assert_eq!(contributors.len(), coordinator.number_of_queue_contributors());
    assert!(contributors.len() > 0);

    for (verifier, proptest_info) in &verifiers {
        proptest_add_verifier(&coordinator, verifier.clone(), proptest_info)?;
    }

    assert_eq!(verifiers.len(), coordinator.number_of_queue_verifiers());
    assert!(verifiers.len() > 0);

    for round in 1..=rounds {
        debug!("Updating ceremony to round {}", round);
        coordinator
            .update()
            .map_err(anyhow::Error::from)
            .with_context(|| format!("error while updating coordinator during round {}", round))?;

        assert_eq!(
            round,
            coordinator
                .current_round_height()
                .map_err(anyhow::Error::from)
                .context("error obtaining current round height")?
        );

        for (contributor, proptest_info) in &contributors {
            tracing::error_span!("{} contribution", %contributor);
            tracing::debug!("making contribution");

            // Calculate how many contributions this contributor needs
            // to make this round.
            let (start_n_assigned_tasks, start_n_completed_tasks) = {
                let contributor_info = coordinator
                    .current_contributor_info(contributor)
                    .map_err(anyhow::Error::from)
                    .with_context(|| {
                        format!(
                            "error obtaining current contributor info for contributor {}",
                            contributor
                        )
                    })?
                    .unwrap_or_else(|| panic!("no current contributor info for contributor {}", contributor));
                (
                    contributor_info.assigned_tasks().len(),
                    contributor_info.completed_tasks().len(),
                )
            };

            // Perform the contributions.
            for _ in 0..start_n_assigned_tasks {
                coordinator
                    .contribute(&contributor, &proptest_info.signing_key, &proptest_info.seed)
                    .map_err(anyhow::Error::from)
                    .with_context(|| format!("error while contributor {} was making contribution", contributor))?;
            }

            assert!(coordinator.is_finished_contributor(contributor));

            // Check that the contributor has finished all their contributions for this round.
            {
                let contributor_info = coordinator
                    .current_contributor_info(contributor)
                    .map_err(anyhow::Error::from)
                    .with_context(|| {
                        format!(
                            "error obtaining current contributor info for contributor {}",
                            contributor
                        )
                    })?
                    .unwrap_or_else(|| panic!("no current contributor info for contributor {}", contributor));
                assert!(contributor_info.assigned_tasks().is_empty());
                assert_eq!(
                    start_n_completed_tasks + start_n_assigned_tasks,
                    contributor_info.completed_tasks().len()
                );
            }
        }

        for (verifier, proptest_info) in &verifiers {
            tracing::error_span!("{} verification", %verifier);
            tracing::debug!("performing verification");

            // Calculate how many verifications need to occur this round.
            let (start_n_assigned_tasks, start_n_completed_tasks) = {
                let verifier_info = coordinator
                    .current_verifier_info(verifier)
                    .map_err(anyhow::Error::from)
                    .with_context(|| format!("error obtaining current verifier info for verifier {}", verifier))?
                    .unwrap_or_else(|| panic!("no current verifier info for verifier {}", verifier));
                (
                    verifier_info.assigned_tasks().len(),
                    verifier_info.completed_tasks().len(),
                )
            };

            // Perform the verifications for this round.
            for _ in 0..start_n_assigned_tasks {
                coordinator
                    .verify(&verifier, &proptest_info.signing_key)
                    .map_err(anyhow::Error::from)
                    .with_context(|| format!("error while verifier {} was performing verification", verifier))?;
            }

            assert!(coordinator.is_finished_verifier(verifier));

            // Check that the verifier has finished all their verification tasks for this round.
            {
                let verifier_info = coordinator.current_verifier_info(verifier)?.unwrap();
                assert!(verifier_info.assigned_tasks().is_empty());
                assert_eq!(
                    start_n_completed_tasks + start_n_assigned_tasks,
                    verifier_info.completed_tasks().len()
                );
            }
        }
    }

    Ok(())
}
