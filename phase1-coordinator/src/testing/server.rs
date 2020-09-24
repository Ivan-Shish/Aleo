use crate::{apis::*, environment::Environment, Coordinator, CoordinatorError};

use tracing::info;

pub fn test_coordinator() -> Result<Coordinator, CoordinatorError> {
    let mut coordinator = Coordinator::new();

    let num_chunks = Environment::Test.number_of_chunks();
    let contributor_ids = vec!["test_contributor".to_string()];
    let verifier_ids = vec!["test_verifier".to_string()];
    let chunk_verifier_ids = (0..num_chunks).into_iter().map(|_| verifier_ids[0].clone()).collect();
    let chunk_verifier_base_urls = (0..num_chunks).into_iter().map(|_| "http://localhost:8080").collect();

    // If this is the first time running the ceremony, start by initializing one round.
    if coordinator.get_round_height()? == 0 {
        coordinator.next_round(
            &contributor_ids,
            &verifier_ids,
            &chunk_verifier_ids,
            &chunk_verifier_base_urls,
        );
    }

    Ok(coordinator)
}

pub fn test_server() -> Result<rocket::Rocket, CoordinatorError> {
    Ok(rocket::ignite().manage(test_coordinator()?).mount("/", routes![
        chunk_get, chunk_post, lock_post, ping_get, // transcript_get,
        round_get,
    ]))
}
