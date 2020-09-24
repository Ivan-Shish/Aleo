use crate::{objects::Round, parameters::BASE_URL, Coordinator, Storage};

use rocket::{http::Status, State};

// TODO (howardwu): Add authentication.
#[get("/chunks/<chunk_id>/contribution", data = "<participant_id>")]
pub fn get_chunk(coordinator: State<Coordinator>, chunk_id: u64, participant_id: String) -> Result<String, Status> {
    let mut round = match coordinator.get_current_round() {
        Ok(round) => round,
        _ => {
            error!("Unable to load the round state");
            return Err(Status::InternalServerError);
        }
    };

    if !round.is_authorized_contributor(participant_id.clone()) {
        error!("Not authorized for /chunks/<chunk_id>/lock");
        return Err(Status::Unauthorized);
    }

    let chunk = match round.get_chunk(chunk_id) {
        Some(chunk) => chunk,
        _ => {
            error!("Unable to load the chunk data");
            return Err(Status::InternalServerError);
        }
    };
    let write_url = format!("{}/{}/contribution/{}", BASE_URL, chunk_id, chunk.version());

    Ok(json!({
        "status": "ok",
        "result": {
            "chunkId": chunk_id,
            "participantId": participant_id,
            "writeUrl": write_url
        }
    })
    .to_string())
}
