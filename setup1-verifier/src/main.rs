use setup1_verifier::{utils::init_logger, verifier::Verifier};

use phase1_coordinator::environment::{Development, Environment, Parameters, Production};
use snarkos_toolkit::account::{Address, ViewKey};

use std::{env, str::FromStr};
use tracing::info;

#[inline]
fn development() -> Environment {
    Development::from(Parameters::TestCustom(64, 16, 512)).into()
}

#[inline]
fn inner() -> Environment {
    Production::from(Parameters::AleoInner).into()
}

#[inline]
fn outer() -> Environment {
    Production::from(Parameters::AleoOuter).into()
}

#[inline]
fn universal() -> Environment {
    Production::from(Parameters::AleoUniversal).into()
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 || args.len() > 5 {
        println!(
            "aleo-setup-verifier {{ 'development', 'inner', 'outer', 'universal' }} {{ COORDINATOR_API_URL }} {{ VERIFIER_VIEW_KEY }} {{ (optional) TRACE }}"
        );
        return;
    }

    let environment = match args[1].as_str() {
        "development" => development(),
        "inner" => inner(),
        "outer" => outer(),
        "universal" => universal(),
        _ => panic!("Invalid environment"),
    };

    let tasks_storage_path = format!("{}_verifier.tasks", args[1].as_str());

    let coordinator_api_url = args[2].clone();

    let view_key = ViewKey::from_str(&args[3]).expect("invalid view key");
    let _address = Address::from_view_key(&view_key).expect("address not derived correctly");

    init_logger();

    // Initialize the verifier
    info!("Initializing verifier...");
    let verifier = Verifier::new(
        coordinator_api_url.to_string(),
        view_key.to_string(),
        environment.into(),
        tasks_storage_path,
    )
    .expect("failed to initialize verifier");

    verifier.start_verifier().await;
}
