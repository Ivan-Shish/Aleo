use phase1_verifier::{
    utils::init_logger,
    verifier::{Verifier, VerifierRequest},
};

use snarkos_toolkit::account::{Address, ViewKey};

use futures_util::StreamExt;
use std::{env, str::FromStr};
use tokio::task;
use tracing::{debug, error, info};
use warp::{ws::WebSocket, Filter, Rejection, Reply};

async fn ws_client_connection(ws: WebSocket, id: String) {
    let (_client_ws_sender, mut client_ws_rcv) = ws.split();

    dotenv::dotenv().ok();

    // Fetch the coordinator api url and verifier view key from the `.env` file
    let coordinator_api_url =
        env::var("COORDINATOR_API_URL").unwrap_or("http://localhost:8000/api/coordinator".to_string());
    let view_key = env::var("VIEW_KEY").unwrap_or("AViewKey1cWNDyYMjc9p78PnCderRx37b9pJr4myQqmmPeCfeiLf3".to_string());

    let verifier = Verifier::new(coordinator_api_url.to_string(), view_key.to_string()).unwrap();

    // The server listens for websocket messages
    while let Some(result) = client_ws_rcv.next().await {
        match result {
            Ok(msg) => {
                debug!("Received message: {:?}", msg);

                if let Ok(message_string) = msg.to_str() {
                    // Check if the message string can be deserialized into a verifier request
                    if let Ok(verifier_request) = serde_json::from_str::<VerifierRequest>(&message_string) {
                        if verifier_request.method.to_lowercase() == "lock" {
                            info!("Attempting to lock chunk {:?}", verifier_request.chunk_id);

                            // Spawn a task to lock the chunk
                            let verifier_clone = verifier.clone();
                            task::spawn(async move {
                                if let Err(err) = verifier_clone.lock_chunk(verifier_request.chunk_id).await {
                                    debug!("Failed to lock chunk (error {})", err);
                                }
                            });
                        } else if verifier_request.method.to_lowercase() == "verify" {
                            info!("Attempting to verify chunk {:?}", verifier_request.chunk_id);

                            // Spawn a task to verify a contribution in the chunk
                            let verifier_clone = verifier.clone();
                            task::spawn(async move {
                                if let Err(err) = verifier_clone.verify_contribution(verifier_request.chunk_id).await {
                                    debug!("Failed to verify chunk (error {})", err);
                                }
                            });
                        }
                    }
                }
            }
            Err(e) => {
                error!("error receiving ws message for id: {}): {}", id.clone(), e);
                break;
            }
        };
    }
}

pub async fn ws_handler(ws: warp::ws::Ws, id: String) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| ws_client_connection(socket, id)))
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    init_logger("TRACE");

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::path::param())
        .and_then(ws_handler);

    // Fetch the coordinator api url and verifier view key from the `.env` file
    let _coordinator_api_url =
        env::var("COORDINATOR_API_URL").expect("COORDINATOR_API_URL environment variable not set");
    let view_key = env::var("VIEW_KEY").expect("VIEW_KEY environment variable not set");

    let view_key = ViewKey::from_str(&view_key).expect("Invalid view key");
    let address = Address::from_view_key(&view_key).expect("Invalid view key. Address not derived correctly");

    info!("Started verifier {} on port 8080", address.to_string());
    warp::serve(ws_route).run(([0, 0, 0, 0], 8080)).await;
}
