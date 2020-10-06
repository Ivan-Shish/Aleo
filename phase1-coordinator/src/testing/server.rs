use crate::{apis::*, environment::Environment, testing::prelude::*, Coordinator, CoordinatorError};

use rocket::{local::Client, Rocket};
use std::sync::Arc;
use tracing::{info, Level};

pub fn test_logger() {
    std::thread::sleep(std::time::Duration::from_secs(1));
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    std::thread::sleep(std::time::Duration::from_secs(1));
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    std::thread::sleep(std::time::Duration::from_secs(1));
}

pub fn test_initialize_to_round_1(coordinator: &Coordinator) -> anyhow::Result<()> {
    // If this is the first time running the ceremony, start by initializing one round.
    if coordinator.current_round_height()? == 0 {
        info!("Starting test initialization of round 0 to round 1");
        coordinator.next_round(
            *TEST_STARTED_AT,
            TEST_CONTRIBUTOR_IDS.to_vec(),
            TEST_VERIFIER_IDS.to_vec(),
        )?;
        info!("Completed test initialization of round 0 to round 1");
    }
    Ok(())
}

pub fn test_coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let coordinator = Coordinator::new(environment.clone())?;
    info!("Coordinator is ready");
    Ok(coordinator)
}

pub fn test_server(environment: &Environment) -> anyhow::Result<(Rocket, Arc<Coordinator>)> {
    info!("Starting server...");
    let coordinator = Arc::new(test_coordinator(environment)?);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let server = rocket::ignite().manage(coordinator.clone()).mount("/", routes![
        chunk_get,
        chunk_post,
        lock_post,
        ping_get,
        timestamp_get,
        round_get,
        deprecated::ceremony_get,
    ]);
    info!("Server is ready");
    Ok((server, coordinator))
}

pub fn test_client(environment: &Environment) -> anyhow::Result<(Client, Arc<Coordinator>)> {
    info!("Starting client");
    let (server, coordinator) = test_server(environment)?;
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Client::new(server).map_err(CoordinatorError::Launch)?;
    info!("Client is ready");
    Ok((client, coordinator))
}
