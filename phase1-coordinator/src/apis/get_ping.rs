#[get("/ping")]
fn ping() -> String {
    format!("pong")
}
