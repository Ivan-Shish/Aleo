#[get("/ping")]
pub fn ping_get() -> String {
    format!("pong")
}

#[cfg(test)]
mod test {
    use crate::testing::prelude::*;

    #[test]
    #[serial]
    fn test_ping_get() {
        let client = test_client().unwrap();

        let mut response = client.get("/ping").dispatch();
        assert_eq!(Status::Ok, response.status());
        assert_eq!(Some("pong".to_string()), response.body_string());
    }
}
