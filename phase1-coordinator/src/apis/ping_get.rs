#[get("/ping")]
pub fn ping_get() -> String {
    format!("pong")
}
