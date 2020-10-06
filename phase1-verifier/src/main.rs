use phase1_verifier::{
    tasks::{task_executor, TaskQueue},
    user_input::input_listener,
    utils::init_logger,
    verifier::{Verifier, VerifierRequest},
};

use snarkos_toolkit::account::{Address, ViewKey};

use futures_util::StreamExt;
use std::{env, str::FromStr};
use tracing::{debug, error, info};
use warp::{ws::WebSocket, Filter};

///
/// Client connection to the coordinator
///
/// Basic message handler logic:
/// 1. Listens for messages that match the request format - `VerifierRequest`
///     Requests:
///         - Lock a chunk with a given `chunk_id`
///         - Verify a chunk with a given `chunk_id`
/// 2. Spawns a tokio task to send a request to the coordinator API endpoints to lock/verify.
/// 3. Return to step 1.
///
async fn ws_client_connection(ws: WebSocket, verifier_tasks: TaskQueue) {
    let (_client_ws_sender, mut client_ws_rcv) = ws.split();

    // The server listens for websocket messages
    while let Some(result) = client_ws_rcv.next().await {
        match result {
            Ok(msg) => {
                debug!("Received message: {:?}", msg);

                if let Ok(message_string) = msg.to_str() {
                    // Check if the message string can be deserialized into a verifier request
                    if let Ok(verifier_request) = serde_json::from_str::<VerifierRequest>(&message_string) {
                        // Add the verifier request to the task queue
                        {
                            debug!("Writing verifier request to task_queue");
                            let mut tasks_lock = verifier_tasks.write().await;
                            tasks_lock.push(verifier_request);
                            drop(tasks_lock);
                            debug!("Wrote verifier request to task_queue");
                        }
                    }
                }
            }
            Err(e) => {
                error!("error receiving ws message: {}", e);
                break;
            }
        };
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    init_logger("DEBUG");

    // Fetch the coordinator api url and verifier view key from the `.env` file
    let coordinator_api_url =
        env::var("COORDINATOR_API_URL").expect("COORDINATOR_API_URL environment variable not set");
    let view_key = env::var("VIEW_KEY").unwrap_or("AViewKey1cWNDyYMjc9p78PnCderRx37b9pJr4myQqmmPeCfeiLf3".to_string());

    let view_key = ViewKey::from_str(&view_key).expect("Invalid view key");
    let address = Address::from_view_key(&view_key).expect("Invalid view key. Address not derived correctly");

    // Initialize the verifier
    let verifier =
        Verifier::new(coordinator_api_url.to_string(), view_key.to_string()).expect("failed to initialize verifier");

    // Keep track of the verifier tasks
    let task_queue = TaskQueue::default();

    // Run the task executor
    let _ = task_executor(verifier.clone(), task_queue.clone()).await;

    // Run the verifier cli listener
    let _ = input_listener(verifier).await;

    // Turn our "task_queue" into a new filter
    let tasks = warp::any().map(move || task_queue.clone());

    // Create the websocket route
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(tasks) // Pass through the task queue as a state to each connection
        .map(|ws: warp::ws::Ws, tasks| {
            // This will call our function if the handshake succeeds.
            ws.on_upgrade(move |socket| ws_client_connection(socket, tasks))
        });

    info!("Started verifier {} on port 8080", address.to_string());
    warp::serve(ws_route).run(([0, 0, 0, 0], 8080)).await;
}
