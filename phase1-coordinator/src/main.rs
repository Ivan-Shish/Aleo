use phase1_coordinator::{
    environment::{Environment, Parameters},
    Coordinator,
    Participant,
};

use std::sync::Arc;
use tracing::info;

#[cfg(not(feature = "silent"))]
#[inline]
fn init_logger() {
    use once_cell::sync::OnceCell;
    use tracing::Level;

    static INSTANCE: OnceCell<()> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    });
}

#[inline]
async fn coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let coordinator = Coordinator::new(environment.clone())?;

    info!("Chunk size is {}", environment.to_settings().5);
    info!("{:#?}", environment.to_settings());

    coordinator.initialize().await?;

    info!("Coordinator is ready");
    info!("{}", serde_json::to_string_pretty(&coordinator.current_round()?)?);

    Ok(coordinator)
}

#[tokio::main]
#[inline]
pub async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "silent"))]
    init_logger();

    coordinator(&Environment::Development(Parameters::AleoTestCustom(8, 12, 256))).await?;

    Ok(())
}
