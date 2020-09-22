use phase1_coordinator::Round;

use rocket::http::Status;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/lock", data = "<participant_id>")]
fn lock(chunk_id: u64, participant_id: String) -> Result<String, Status> {
    let mut round = match get_round() {
        Ok(round) => round,
        _ => {
            error!("Unable to load the round state");
            return Err(Status::InternalServerError);
        }
    };

    if !round.is_authorized(participant_id.clone()) {
        error!("Not authorized for /chunks/<chunk_id>/lock");
        return Err(Status::Unauthorized);
    }

    // TODO (howardwu): Account for the case where the participant is already the lock holder.
    let is_locked = round.try_lock_chunk(chunk_id, participant_id);
    Ok(json!({
        "status": "ok",
        "result": {
            "chunkId": chunk_id,
            "locked": is_locked
        }
    })
    .to_string())
}
