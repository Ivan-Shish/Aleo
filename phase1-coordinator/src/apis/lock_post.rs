use crate::{objects::Round, Coordinator, Storage};

use rocket::{http::Status, State};
use tracing::error;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/lock", data = "<participant_id>")]
pub fn lock_post(coordinator: State<Coordinator>, chunk_id: u64, participant_id: String) -> Result<String, Status> {
    match coordinator.lock_chunk(chunk_id, participant_id) {
        Ok(_) => Ok(json!({
            "status": "ok",
            "result": {
                "chunkId": chunk_id,
                "locked": true
            }
        })
        .to_string()),
        _ => {
            error!("Unable to load the round state");
            Err(Status::InternalServerError)
        }
    }
    //
    // if !round.is_authorized_contributor(participant_id.clone()) {
    //     error!("Not authorized for /chunks/{}/lock", chunk_id);
    //     return Err(Status::Unauthorized);
    // }
    //
    // // TODO (howardwu): Account for the case where the participant is already the lock holder.
    // let is_locked = round.try_lock_chunk(chunk_id, participant_id);
}
