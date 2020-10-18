use phase1_coordinator::{
    environment::{DevelopmentEnvironment, Environment, Parameters},
    Coordinator,
};

use std::time::Duration;
use tokio::{task, time::delay_for};
use tracing::*;

#[cfg(not(feature = "silent"))]
#[inline]
fn init_logger() {
    use once_cell::sync::OnceCell;

    static INSTANCE: OnceCell<()> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::DEBUG).finish();
        tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    });
}

#[inline]
async fn coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    Ok(Coordinator::new(environment.clone())?)
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "silent"))]
    init_logger();

    // Set the environment.
    let environment = (*DevelopmentEnvironment::from(Parameters::TestCustom(8, 12, 256)).clone()).clone();

    info!("{:#?}", environment.parameters());

    // Instantiate the coordinator.
    let coordinator = coordinator(&environment).await?;

    // Initialize the coordinator.
    let operator = coordinator.clone();
    let ceremony = task::spawn(async move {
        // Initialize the coordinator.
        operator.initialize().await.unwrap();

        // Initialize the coordinator loop.
        loop {
            // Run the update operation.
            if let Err(error) = operator.update().await {
                error!("{}", error);
            }

            // Sleep for 10 seconds in between iterations.
            delay_for(Duration::from_secs(10)).await;
        }
    });

    // Initialize the shutdown procedure.
    let handler = coordinator.clone();
    {
        debug!("Initializing the shutdown handler");
        handler.shutdown_listener()?;
    }

    ceremony.await.expect("The ceremony handle has panicked");

    Ok(())
}
