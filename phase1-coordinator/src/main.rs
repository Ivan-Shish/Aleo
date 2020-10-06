#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use phase1_coordinator::{
    apis::*,
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
use tracing::{info, Level};

#[inline]
fn logger() {
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[inline]
fn coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let coordinator = Coordinator::new(environment.clone())?;

    let contributors = vec![Participant::Contributor(
        "0xd0FaDc3C5899c28c581c0e06819f4113cb08b0e4".to_string(),
    )];
    let verifiers = vec![environment.coordinator_verifier()];

    // If this is the first time running the ceremony, start by initializing one round.
    if coordinator.current_round_height()? == 0 {
        coordinator.next_round(Utc::now(), contributors, verifiers)?;
    }
    info!("Coordinator is ready");
    info!("{}", serde_json::to_string_pretty(&coordinator.current_round()?)?);

    Ok(coordinator)
}

#[inline]
fn server(environment: &Environment) -> anyhow::Result<Rocket> {
    info!("Starting server...");
    info!("Chunk size is {}", environment.to_settings().5);
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
        .manage(Arc::new(coordinator(environment)?))
        .mount("/", routes![
            chunk_get,
            chunk_post,
            lock_post,
            ping_get,
            timestamp_get,
            round_get,
            deprecated::ceremony_get,
        ])
        .attach(environment.cors());
    info!("Server is ready");
    Ok(server)
}

#[inline]
pub fn main() -> anyhow::Result<()> {
    logger();
    server(&Environment::Test(Parameters::AleoTestCustom(16, 12, 256)))?.launch();
    Ok(())
}
