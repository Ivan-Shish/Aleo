use crate::{objects::Participant, Coordinator};

use rocket::{http::Status, State};
use tracing::error;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/contribution", data = "<participant>")]
pub fn chunk_post(
    coordinator: State<Coordinator>,
    chunk_id: u64,
    participant: Participant,
    // contribution_id: u64,
) -> Result<String, Status> {
    match coordinator.contribute_chunk(chunk_id, participant) {
        Ok(_) => Ok(json!({ "status": "ok" }).to_string()),
        Err(_) => {
            error!("Unable to store the contribution");
            Err(Status::BadRequest)
        }
    }
    // Err(Status::BadRequest)
}
