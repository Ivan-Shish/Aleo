use phase1_coordinator::Round;

use rocket::http::Status;
use url::Url;

// TODO (howardwu): Add authentication.
#[post("/chunks/<chunk_id>/contribution", data = "<participant_id>")]
fn post_chunk(chunk_id: u64, participant_id: String) -> Result<String, Status> {
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

    let mut chunk = match round.get_chunk(chunk_id) {
        Some(chunk) => chunk,
        _ => {
            error!("Unable to load the chunk data");
            return Err(Status::InternalServerError);
        }
    };
    let copy_url = match Url::parse(&format!("{}/{}/contribution/{}", BASE_URL, chunk_id, chunk.version())) {
        Ok(round) => round,
        _ => {
            error!("Unable to parse the URL");
            return Err(Status::BadRequest);
        }
    };

    match round.contribute_chunk(chunk_id, participant_id, copy_url) {
        true => Ok(json!({ "status": "ok" }).to_string()),
        false => {
            error!("Unable to store the contribution");
            Err(Status::BadRequest)
        }
    }
}
