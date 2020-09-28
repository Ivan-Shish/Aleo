use crate::{objects::Participant, Coordinator};

use rocket::{http::Status, State};
use tracing::error;

// TODO (howardwu): Add authentication.
#[get("/chunks/<chunk_id>/contribution", data = "<participant>")]
pub fn chunk_get(coordinator: State<Coordinator>, chunk_id: u64, participant: Participant) -> Result<String, Status> {
    // Check that the participant ID is authorized.
    if !coordinator.is_current_contributor(&participant) {
        error!("{} is not authorized for chunk {}", participant, chunk_id);
        return Err(Status::Unauthorized);
    }

    // Return the next contribution locator for the given chunk ID.
    match coordinator.next_contribution_locator_strict(chunk_id) {
        Ok(contribution_locator) => {
            let response = json!({
                "status": "ok",
                "result": {
                    "chunkId": chunk_id,
                    "participantId": participant,
                    "writeUrl": contribution_locator.to_string()
                }
            });
            Ok(response.to_string())
        }
        _ => return Err(Status::Unauthorized),
    }
}
