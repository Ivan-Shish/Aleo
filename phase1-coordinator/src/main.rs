#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_json;

use phase1_coordinator::apis::*;

const BASE_URL: &str = "http://167.71.156.62:8080";

// #[derive(Debug, Deserialize, Serialize, JsonSchema)]
// pub struct LockRequest {
//     participant_id: String,
// }

// TODO (howardwu): Implement the real version.
fn get_round() -> Result<Round, Box<dyn std::error::Error>> {
    let result: Round = serde_json::from_str(include_str!("./resources/ceremony.json"))?;
    Ok(result)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    rocket::ignite()
        .mount("/", routes![get_ceremony, post_lock, get_chunk, post_chunk, get_ping])
        .launch();
    Ok(())
}
