#[get("/ping")]
pub fn ping() -> String {
    format!("pong")
}
