use phase1_coordinator::{
    authentication::{Dummy, Signature},
    environment::{Development, Environment, Parameters},
    storage::Disk,
    Coordinator,
};

use std::{sync::Arc, time::Duration};
use tokio::{
    sync::RwLock,
    task::{self},
    time::sleep,
};
use tracing::*;

#[inline]
async fn coordinator(environment: &Environment, signature: Arc<dyn Signature>) -> anyhow::Result<Coordinator<Disk>> {
    Ok(Coordinator::new(environment.clone(), signature)?)
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Set the environment.
    let environment: Environment = Development::from(Parameters::TestCustom {
        number_of_chunks: 8,
        power: 12,
        batch_size: 256,
    })
    .into();
    // use phase1_coordinator::environment::Production;
    // let environment: Environment = Production::from(Parameters::AleoInner).into();

    // Instantiate the coordinator.
    let coordinator = Arc::new(RwLock::new(coordinator(&environment, Arc::new(Dummy)).await?));

    let ceremony_coordinator = coordinator.clone();
    // Initialize the coordinator.
    let ceremony = task::spawn(async move {
        // Initialize the coordinator.
        ceremony_coordinator.write().await.initialize().unwrap();

        // Initialize the coordinator loop.
        loop {
            // Run the update operation.
            if let Err(error) = ceremony_coordinator.write().await.update() {
                error!("{}", error);
            }

            // Sleep for 10 seconds in between iterations.
            sleep(Duration::from_secs(10)).await;
        }
    });

    // Initialize the shutdown procedure.
    debug!("Initializing the shutdown handler");
    coordinator.write().await.shutdown_listener()?;

    ceremony.await.expect("The ceremony handle has panicked");

    Ok(())
}
