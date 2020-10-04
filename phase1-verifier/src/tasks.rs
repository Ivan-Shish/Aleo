use crate::verifier::{Verifier, VerifierRequest};

use std::{env, sync::Arc, thread::sleep, time::Duration};
use tokio::{sync::RwLock, task};
use tracing::{debug, info};

pub type TaskQueue = Arc<RwLock<Vec<VerifierRequest>>>;

pub async fn task_executor(verifier_tasks: TaskQueue) {
    dotenv::dotenv().ok();

    // Fetch the coordinator api url and verifier view key from the `.env` file
    // TODO Remove hardcoded values
    let coordinator_api_url = env::var("COORDINATOR_API_URL").unwrap_or("http://localhost:8000/api".to_string());
    let view_key = env::var("VIEW_KEY").unwrap_or("AViewKey1cWNDyYMjc9p78PnCderRx37b9pJr4myQqmmPeCfeiLf3".to_string());

    let verifier = Verifier::new(coordinator_api_url.to_string(), view_key.to_string()).unwrap();

    // Spawn a thread to run the worker to run tasks from the queue.
    task::spawn(async move {
        let tasks = verifier_tasks.clone();
        loop {
            // Wrap a closure around decing
            {
                let mut tasks_lock = tasks.write().await;

                if !tasks_lock.is_empty() {
                    let verifier_request: VerifierRequest = tasks_lock[0].clone();

                    if verifier_request.method.to_lowercase() == "lock" {
                        info!("Attempting to lock chunk {:?}", verifier_request.chunk_id);

                        // Spawn a task to lock the chunk
                        let verifier_clone = verifier.clone();
                        task::spawn(async move {
                            match verifier_clone.lock_chunk(verifier_request.chunk_id).await {
                                Ok(_) => {
                                    info!("Locked chunk {:?}", verifier_request.chunk_id);
                                }
                                Err(err) => {
                                    debug!("Failed to lock chunk (error {})", err);
                                }
                            }
                        });
                    } else if verifier_request.method.to_lowercase() == "verify" {
                        info!("Attempting to verify chunk {:?}", verifier_request.chunk_id);

                        // Spawn a task to verify the chunk
                        let verifier_clone = verifier.clone();
                        task::spawn(async move {
                            match verifier_clone.verify_contribution(verifier_request.chunk_id).await {
                                Ok(_) => {
                                    info!("Verified chunk {:?}", verifier_request.chunk_id);
                                }
                                Err(err) => {
                                    debug!("Failed to verify chunk (error {})", err);
                                }
                            }
                        });
                    }
                    // Remove the task from the task queue
                    tasks_lock.pop();
                }

                // Drop the tasks write lock
                drop(tasks_lock)
            }
            // Sleep for 2 seconds
            sleep(Duration::from_secs(2));
        }
    });
}
