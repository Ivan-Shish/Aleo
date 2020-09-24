#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_json;

use phase1_coordinator::{apis::*, storage::InMemory, Coordinator};

// use tokio::prelude::*;

// #[derive(Debug, Deserialize, Serialize, JsonSchema)]
// pub struct LockRequest {
//     participant_id: String,
// }

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = Coordinator::new();

    rocket::ignite()
        .manage(coordinator)
        .mount("/", routes![get_ceremony, post_lock, get_chunk, post_chunk, ping])
        .launch();

    Ok(())
}
