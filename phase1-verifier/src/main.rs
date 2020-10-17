use phase1_coordinator::environment::{DevelopmentEnvironment, Parameters};
use phase1_verifier::{utils::init_logger, verifier::Verifier};

use snarkos_toolkit::account::{Address, ViewKey};

use std::{env, str::FromStr};
use tracing::info;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    init_logger("TRACE");

    // Fetch the coordinator api url and verifier view key from the `.env` file
    let coordinator_api_url =
        env::var("COORDINATOR_API_URL").expect("COORDINATOR_API_URL environment variable not set");
    let view_key = env::var("VIEW_KEY").unwrap_or("AViewKey1cWNDyYMjc9p78PnCderRx37b9pJr4myQqmmPeCfeiLf3".to_string());

    let view_key = ViewKey::from_str(&view_key).expect("Invalid view key");
    let _address = Address::from_view_key(&view_key).expect("Invalid view key. Address not derived correctly");

    let environment = DevelopmentEnvironment::from(Parameters::Test3Chunks);

    // Initialize the verifier
    info!("Initializing verifier...");
    let verifier = Verifier::new(coordinator_api_url.to_string(), view_key.to_string(), environment)
        .expect("failed to initialize verifier");

    verifier.start_verifier().await;
}
