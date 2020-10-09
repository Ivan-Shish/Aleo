use crate::{
    environment::{Environment, Parameters},
    objects::Round,
    storage::Storage,
    Participant,
};

use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;
use serde_diff::{Diff, SerdeDiff};
use serial_test::serial;
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use tracing::{error, warn};

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT: Environment = Environment::Test(Parameters::AleoTest8Chunks);

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT_3: Environment = Environment::Test(Parameters::AleoTest3Chunks);

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT_20: Environment = Environment::Test(Parameters::AleoTest20Chunks);

lazy_static! {
    /// Round start datetime for testing purposes only.
    pub static ref TEST_STARTED_AT: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 1, 1);

    /// Contributor ID for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_ID: Lazy<Participant> = Lazy::new(|| Participant::Contributor(format!("test-coordinator-contributor")));

    /// Contributor ID 2 for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_ID_2: Lazy<Participant> = Lazy::new(|| Participant::Contributor(format!("test-coordinator-contributor-2")));

    /// Contributor ID 3 for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_ID_3: Lazy<Participant> = Lazy::new(|| Participant::Contributor(format!("test-coordinator-contributor-3")));

    /// Verifier ID for testing purposes only.
    pub static ref TEST_VERIFIER_ID: Lazy<Participant> = Lazy::new(|| Participant::Verifier(format!("test-coordinator-verifier")));

    /// Verifier ID 2 for testing purposes only.
    pub static ref TEST_VERIFIER_ID_2: Lazy<Participant> = Lazy::new(|| Participant::Verifier(format!("test-coordinator-verifier-2")));

    /// Contributor IDs for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_IDS: Lazy<Vec<Participant>> = Lazy::new(|| vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()]);

    /// Verifier IDs for testing purposes only.
    pub static ref TEST_VERIFIER_IDS: Lazy<Vec<Participant>> =  Lazy::new(|| vec![Lazy::force(&TEST_VERIFIER_ID).clone()]);
}

/// Clears the transcript directory for testing purposes only.
pub fn clear_test_transcript() {
    let path = TEST_ENVIRONMENT.local_base_directory();
    if Path::new(path).exists() {
        warn!("Coordinator is clearing {:?}", &path);
        match std::fs::remove_dir_all(&path) {
            Ok(_) => (),
            Err(error) => error!(
                "The testing framework tried to clear the test transcript and failed. {}",
                error
            ),
        }
        warn!("Coordinator cleared {:?}", &path);
    }
}

/// Provides a simple test storage object.
pub fn test_storage() -> Arc<RwLock<Box<dyn Storage>>> {
    Arc::new(RwLock::new(TEST_ENVIRONMENT.storage().unwrap()))
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_0_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!("resources/test_round_0.json"))?)
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_1_initial_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!(
        "resources/test_round_1_initial.json"
    ))?)
}

/// Creates the initial round for testing purposes only.
pub fn test_round_0() -> anyhow::Result<Round> {
    // Define test storage.
    let test_storage = test_storage();
    let storage = test_storage.write().unwrap();

    Ok(Round::new(
        &TEST_ENVIRONMENT,
        &storage,
        0, /* height */
        *TEST_STARTED_AT,
        vec![],
        TEST_VERIFIER_IDS.to_vec(),
    )?)
}

/// Prints the difference in JSON objects between `a` and `b`.
pub fn print_diff<S: SerdeDiff>(a: &S, b: &S) {
    println!(
        "\nDifference(s) between left and right values\n-------------------------------------------\n{}\n",
        serde_json::to_string_pretty(&Diff::serializable(a, b)).unwrap()
    );
}

#[test]
#[serial]
fn test_round_0_matches() {
    let expected = test_round_0_json().unwrap();
    let candidate = test_round_0().unwrap();

    // Print the differences in JSON if they do not match.
    if candidate != expected {
        print_diff(&expected, &candidate);
    }
    assert_eq!(candidate, expected);
}
