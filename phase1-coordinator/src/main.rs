#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_json;

use phase1_coordinator::{apis::*, environment::Environment, Coordinator, CoordinatorError};

use tracing::{info, Level};
// use tokio::prelude::*;

// #[derive(Debug, Deserialize, Serialize, JsonSchema)]
// pub struct LockRequest {
//     participant_id: String,
// }

// Initialize the global subscriber.
pub fn logger() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        // build but do not install the subscriber.
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

pub fn initialize_coordinator() -> Result<Coordinator, CoordinatorError> {
    let mut coordinator = Coordinator::new();

    let num_chunks = Environment::Development.number_of_chunks();
    let contributor_ids = vec!["development_contributor".to_string()];
    let verifier_ids = vec!["development_verifier".to_string()];
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

    info!(
        "{}",
        serde_json::to_string_pretty(&coordinator.get_latest_round()?).unwrap()
    );

    Ok(coordinator)
}

pub fn server() -> Result<rocket::Rocket, CoordinatorError> {
    Ok(rocket::ignite().manage(initialize_coordinator()?).mount("/", routes![
        chunk_get, chunk_post, lock_post, ping_get, // transcript_get,
        round_get,
    ]))
}

pub fn main() -> Result<(), CoordinatorError> {
    logger();
    server()?.launch();
    Ok(())
}
