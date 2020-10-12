use crate::{environment::Environment, testing::prelude::*, Coordinator, CoordinatorError, Participant, Storage};

use once_cell::sync::Lazy;
use rocket::{local::Client, Rocket};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use tracing::{error, info, warn};

pub fn initialize_test_environment() {
    #[cfg(not(feature = "silent"))]
    test_logger();

    clear_test_storage();
}

pub fn initialize_coordinator(
    coordinator: &Coordinator,
    contributors: Vec<Participant>,
    verifiers: Vec<Participant>,
) -> anyhow::Result<()> {
    // Ensure the ceremony has not started.
    assert_eq!(0, coordinator.current_round_height()?);

    // Run initialization.
    coordinator.next_round(*TEST_STARTED_AT, contributors, verifiers)?;

    // Check current round height is now 1.
    assert_eq!(1, coordinator.current_round_height()?);

    std::thread::sleep(std::time::Duration::from_secs(1));
    Ok(())
}

#[cfg(not(feature = "silent"))]
pub fn test_logger() {
    use once_cell::sync::OnceCell;
    use tracing::Level;

    static INSTANCE: OnceCell<()> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    });
}

/// Clears the transcript directory for testing purposes only.
pub fn clear_test_storage() {
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

/// Initializes a test storage object.
pub fn test_storage(environment: &Environment) -> Arc<RwLock<Box<dyn Storage>>> {
    Arc::new(RwLock::new(environment.storage().unwrap()))
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
    let server = rocket::ignite().manage(coordinator.clone()).mount("/", routes![]);
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
