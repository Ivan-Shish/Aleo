use crate::{apis::*, environment::Environment, testing::prelude::*, Coordinator, CoordinatorError};

use rocket::{
    http::{ContentType, Status},
    local::Client,
    Rocket,
};
use tracing::info;

pub fn test_coordinator() -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let mut coordinator = Coordinator::new(Environment::Test);
    // If this is the first time running the ceremony, start by initializing one round.
    if coordinator.current_round_height()? == 0 {
        coordinator.next_round(
            *TEST_STARTED_AT,
            &TEST_CONTRIBUTOR_IDS,
            &TEST_VERIFIER_IDS,
            &TEST_CHUNK_VERIFIER_IDS,
            &TEST_CHUNK_VERIFIED_BASE_URLS,
        )?;
    }
    info!("Coordinator is ready");
    Ok(coordinator)
}

pub fn test_server() -> anyhow::Result<Rocket> {
    info!("Starting server...");
    let server = rocket::ignite().manage(test_coordinator()?).mount("/", routes![
        chunk_get,
        chunk_post,
        lock_post,
        ping_get,
        timestamp_get, // transcript_get,
        round_get,
    ]);
    info!("Server is ready");
    Ok(server)
}

pub fn test_client() -> anyhow::Result<Client> {
    info!("Starting client");
    let client = Client::new(test_server()?).map_err(CoordinatorError::Launch)?;
    info!("Client is ready");
    Ok(client)
}
