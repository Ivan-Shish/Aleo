use crate::{objects::Participant, Coordinator};

use rocket::{http::Status, State};
use std::sync::Arc;
use tracing::error;

// TODO (howardwu): Add authentication.
#[get("/chunks/<chunk_id>/contribution", data = "<participant>")]
pub fn chunk_get(
    coordinator: State<Arc<Coordinator>>,
    chunk_id: u64,
    participant: Participant,
) -> Result<String, Status> {
    // Check that the participant ID is authorized.
    match coordinator.current_round() {
        Ok(current_round) => {
            if !current_round.is_authorized_contributor(&participant) {
                error!("{} is not authorized for chunk {}", participant, chunk_id);
                return Err(Status::Unauthorized);
            }
            // Return the next contribution locator for the given chunk ID.
            match current_round.next_contribution_locator(coordinator.environment(), chunk_id) {
                Ok(contribution_locator) => Ok(json!({
                    "status": "ok",
                    "result": {
                        "chunkId": chunk_id,
                        "participantId": participant,
                        "writeUrl": contribution_locator.to_string()
                    }
                })
                .to_string()),
                _ => return Err(Status::Unauthorized),
            }
        }
        _ => return Err(Status::Unauthorized),
    }
}
