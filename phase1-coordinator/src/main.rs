use phase1_coordinator::{
    environment::{Environment, Parameters},
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
    let environment = Environment::Development(Parameters::AleoTestCustom(8, 12, 256));

    // Instantiate the coordinator.
    let coordinator = coordinator(&environment).await?;

    // Initialize the coordinator.
    let operator = coordinator.clone();
    {
        task::spawn(async move {
            info!("Chunk size is {}", environment.to_settings().5);
            info!("{:#?}", environment.to_settings());

            operator.initialize().await.unwrap();
        });
    }

    // Initialize the backup procedure.
    {
        task::spawn(async move {
            loop {
                info!("Backing up coordinator (UNIMPLEMENTED)");
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
