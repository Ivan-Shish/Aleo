use crate::{Coordinator, Storage};

use rocket::{http::Status, State};

#[get("/transcript")]
pub fn transcript_get(coordinator: State<Coordinator>) -> Result<String, Status> {
    let round = match coordinator.get_transcript() {
        Ok(round) => round,
        _ => return Err(Status::InternalServerError),
    };
    match serde_json::to_string_pretty(&round) {
        Ok(json) => Ok(json),
        _ => Err(Status::InternalServerError),
    }
}
