#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use phase1_coordinator::{
    environment::{Environment, Parameters},
    Coordinator,
    Participant,
};

use chrono::Utc;
use rocket::{
    config::{Config, Environment as RocketEnvironment},
    Rocket,
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

#[inline]
async fn server(environment: &Environment) -> anyhow::Result<Rocket> {
    info!("Starting server...");

    let builder = match environment {
        Environment::Test(_) => Config::build(RocketEnvironment::Development),
        Environment::Development(_) => Config::build(RocketEnvironment::Production),
        Environment::Production(_) => Config::build(RocketEnvironment::Production),
    };

    let config = builder
        .address(environment.address())
        .port(environment.port())
        .finalize()?;

    let server = rocket::custom(config)
        .manage(Arc::new(coordinator(environment).await?))
        .mount("/", routes![])
        .attach(environment.cors());

    info!("Server is ready");
    Ok(server)
}

#[tokio::main]
#[inline]
pub async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "silent"))]
    init_logger();

    server(&Environment::Development(Parameters::AleoTestCustom(8, 12, 256)))
        .await?
        .launch();
    Ok(())
}
