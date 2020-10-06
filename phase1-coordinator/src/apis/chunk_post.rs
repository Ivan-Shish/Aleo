use crate::{objects::Participant, Coordinator};

use rocket::{http::Status, State};
use std::sync::Arc;
use tracing::error;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/contribution", data = "<participant>")]
pub fn chunk_post(
    coordinator: State<Arc<Coordinator>>,
    chunk_id: u64,
    participant: Participant,
    // contribution_id: u64,
) -> Result<String, Status> {
    match coordinator.add_contribution(chunk_id, &participant) {
        Ok(_) => Ok(json!({ "status": "ok" }).to_string()),
        Err(_) => {
            error!("Unable to store the contribution");
            Err(Status::BadRequest)
        }
    }
}
