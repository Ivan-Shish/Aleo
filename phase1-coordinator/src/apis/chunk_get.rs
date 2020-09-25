use crate::Coordinator;

use rocket::{http::Status, State};
use tracing::error;

// TODO (howardwu): Add authentication.
#[get("/chunks/<chunk_id>/contribution", data = "<participant_id>")]
pub fn chunk_get(coordinator: State<Coordinator>, chunk_id: u64, participant_id: String) -> Result<String, Status> {
    // Check that the participant ID is authorized.
    match coordinator.is_current_contributor(participant_id.clone()) {
        // Case 1 - Participant is authorized.
        Ok(true) => (),
        // Case 2 - Participant is not authorized.
        Ok(false) => {
            error!(
                "{} is not authorized for /chunks/{}/lock",
                participant_id.clone(),
                chunk_id
            );
            return Err(Status::Unauthorized);
        }
        // Case 3 - Ceremony has not started, or storage has been corrupted.
        Err(_) => return Err(Status::InternalServerError),
    }

    // Fetch the current round height from storage.
    let round_height = match coordinator.current_round_height() {
        Ok(height) => height,
        _ => return Err(Status::InternalServerError),
    };

    // Return the chunk locator hash for the given chunk ID.
    match coordinator.get_chunk_locator(round_height, chunk_id) {
        Ok(chunk_locator) => {
            let response = json!({
                "status": "ok",
                "result": {
                    "chunkId": chunk_id,
                    "participantId": participant_id,
                    "writeUrl": chunk_locator.to_string()
                }
            });
            Ok(response.to_string())
        }
        _ => return Err(Status::InternalServerError),
    }
}
