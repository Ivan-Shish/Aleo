use phase1_coordinator::{
    environment::{DevelopmentEnvironment, Environment, Parameters},
    Coordinator,
};

use tokio::task;
use tracing::info;

#[cfg(not(feature = "silent"))]
#[inline]
fn init_logger() {
    use once_cell::sync::OnceCell;
    use tracing::Level;

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
#[inline]
pub async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "silent"))]
    init_logger();

    // Set the environment.
    let environment = (*DevelopmentEnvironment::from(Parameters::TestCustom(8, 12, 256)).clone()).clone();

    // Instantiate the coordinator.
    let coordinator = coordinator(&environment).await?;

    // Initialize the coordinator.
    let operator = coordinator.clone();
    {
        info!("Chunk size is {}", environment.parameters().5);
        info!("{:#?}", environment.parameters());

        operator.initialize().await.unwrap();

        task::spawn(async move {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        });
    }

    // Initialize the shutdown procedure.
    let handler = coordinator.clone();
    {
        handler.shutdown_listener()?;
    }

    Ok(())
}
