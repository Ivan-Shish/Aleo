use crate::{environment::Environment, testing::prelude::*, Coordinator, CoordinatorError, Participant, Storage};

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
