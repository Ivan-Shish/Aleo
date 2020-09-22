use rocket::http::Status;

#[get("/ceremony")]
fn ceremony() -> Result<String, Status> {
    let round = match COORDINATOR::load_current_round() {
        Ok(round) => round,
        _ => return Err(Status::InternalServerError),
    };
    match serde_json::to_string_pretty(&round) {
        Ok(json) => Ok(json),
        _ => Err(Status::InternalServerError),
    }
}
