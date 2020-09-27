use crate::{
    environment::{Environment, Parameters},
    objects::Round,
    Participant,
};

use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;
use serde_diff::{Diff, SerdeDiff};

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT: Environment = Environment::Test(Parameters::AleoTest);

lazy_static! {
    /// Round start datetime for testing purposes only.
    pub static ref TEST_STARTED_AT: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 1, 1);

    /// Contributor ID 1 for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_ID_1: Lazy<Participant> = Lazy::new(|| Participant::Contributor(format!("test_contributor")));

    /// Verifier ID 1 for testing purposes only.
    pub static ref TEST_VERIFIER_ID_1: Lazy<Participant> = Lazy::new(|| Participant::Verifier(format!("test_verifier")));

    /// Verified base URL 1 for testing purposes only.
    pub static ref TEST_VERIFIED_BASE_URL_1: Lazy<String> = Lazy::new(|| format!("http://localhost:8080"));

    /// Contributor IDs for testing purposes only.
    pub static ref TEST_CONTRIBUTOR_IDS: Lazy<Vec<Participant>> = Lazy::new(|| vec![Lazy::force(&TEST_CONTRIBUTOR_ID_1).clone()]);

    /// Verifier IDs for testing purposes only.
    pub static ref TEST_VERIFIER_IDS: Lazy<Vec<Participant>> =  Lazy::new(|| vec![Lazy::force(&TEST_VERIFIER_ID_1).clone()]);

    /// Chunk verifier IDs for testing purposes only.
    pub static ref TEST_CHUNK_VERIFIER_IDS: Lazy<Vec<Participant>> = Lazy::new(|| (0..TEST_ENVIRONMENT.number_of_chunks()).into_iter().map(|_| Lazy::force(&TEST_VERIFIER_IDS)[0].clone()).collect());

    /// Chunk verified base URLs for testing purposes only.
    pub static ref TEST_CHUNK_VERIFIED_BASE_URLS: Lazy<Vec<String>> = Lazy::new(|| (0..TEST_ENVIRONMENT.number_of_chunks()).into_iter().map(|_| Lazy::force(&TEST_VERIFIED_BASE_URL_1).clone()).collect());
}

/// Clears the transcript directory for testing purposes only.
pub fn clear_test_transcript() {
    // std::thread::sleep(1000);
    for round_height in 0..10 {
        let path = TEST_ENVIRONMENT.round_directory(round_height);
        if std::path::Path::new(&path).exists() {
            // warn!("Test is clearing {:?}", &path);
            std::fs::remove_dir_all(&path).expect("unable to remove transcript directory");
            // warn!("Test cleared {:?}", &path);
        }
    }
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_0_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!("resources/test_round_0.json"))?)
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_1_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!("resources/test_round_1.json"))?)
}

/// Creates the initial round for testing purposes only.
pub fn test_round_0() -> anyhow::Result<Round> {
    Ok(Round::new(
        &TEST_ENVIRONMENT,
        0, /* height */
        *TEST_STARTED_AT,
        TEST_CONTRIBUTOR_IDS.to_vec(),
        TEST_VERIFIER_IDS.to_vec(),
        TEST_CHUNK_VERIFIER_IDS.to_vec(),
        TEST_CHUNK_VERIFIED_BASE_URLS.to_vec(),
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
fn test_round_0_matches() {
    let expected = test_round_0_json().unwrap();
    let candidate = test_round_0().unwrap();

    // Print the differences in JSON if they do not match.
    if candidate != expected {
        print_diff(&expected, &candidate);
    }
    assert_eq!(candidate, expected);
}
