use crate::{objects::Participant, Coordinator, CoordinatorError};

use rocket::{http::Status, State};
use rocket_contrib::json::Json;
use serde::{Deserialize, Serialize};
use serde_diff::SerdeDiff;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, SerdeDiff)]
pub struct InnerLockResponse {
    chunk_id: u64,
    locked: bool,
}

#[derive(Debug, Serialize, Deserialize, SerdeDiff)]
pub struct LockResponse {
    status: String,
    result: InnerLockResponse,
}

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/lock", data = "<participant>")]
pub fn lock_post(
    coordinator: State<Arc<Coordinator>>,
    chunk_id: u64,
    participant: Participant,
) -> Result<Json<LockResponse>, Status> {
    // TODO (howardwu): Account for the case where the participant is already the lock holder.
    match coordinator.try_lock_chunk(chunk_id, &participant) {
        Ok(_) => Ok(Json(LockResponse {
            status: format!("ok"),
            result: InnerLockResponse { chunk_id, locked: true },
        })),
        Err(CoordinatorError::UnauthorizedChunkContributor) => Err(Status::Unauthorized),
        Err(CoordinatorError::ChunkLockAlreadyAcquired) => Err(Status::Unauthorized),
        Err(CoordinatorError::ChunkMissing) => Err(Status::PreconditionFailed),
        _ => Err(Status::NotFound),
    }
}
