use crate::verifier::{Verifier, VerifierRequest};

use std::{sync::Arc, time::Duration};
use tokio::{sync::RwLock, task, time::delay_for};
use tracing::{debug, info};

pub type TaskQueue = Arc<RwLock<Vec<VerifierRequest>>>;

///
/// The task executor reads from the list of VerifierRequest tasks
/// and sequentially executes them. It dispatches a lock or verify
/// request to the coordinator based on the VerifierRequest task
///
pub async fn task_executor(verifier: Verifier, verifier_tasks: TaskQueue) {
    // Spawn a thread to run the worker to run tasks from the queue.
    task::spawn(async move {
        let tasks = verifier_tasks.clone();
        loop {
            // Wrap a closure around verifier task operations to enforce locks are dropped
            {
                let mut tasks_lock = tasks.write().await;

                if !tasks_lock.is_empty() {
                    let verifier_request: VerifierRequest = tasks_lock[0].clone();

                    if verifier_request.method.to_lowercase() == "lock" {
                        info!("Attempting to lock chunk {:?}", verifier_request.chunk_id);

                        match verifier.lock_chunk(verifier_request.chunk_id).await {
                            Ok(_) => {
                                info!("Locked chunk {:?}", verifier_request.chunk_id);
                            }
                            Err(err) => {
                                debug!("Failed to lock chunk {} (error {})", verifier_request.chunk_id, err);
                            }
                        }
                    } else if verifier_request.method.to_lowercase() == "verify" {
                        info!("Attempting to verify chunk {:?}", verifier_request.chunk_id);

                        // Spawn a task to verify the chunk
                        match verifier.verify_contribution(verifier_request.chunk_id).await {
                            Ok(_) => {
                                info!("Verified chunk {:?}", verifier_request.chunk_id);
                            }
                            Err(err) => {
                                debug!("Failed to verify chunk {} (error {})", verifier_request.chunk_id, err);
                            }
                        }
                    }
                    // Remove the task from the task queue
                    tasks_lock.remove(0);
                    drop(tasks_lock);
                }
            }
            // Sleep for 3 seconds
            delay_for(Duration::from_secs(3)).await;
        }
    });
}
