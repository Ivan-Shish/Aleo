use crate::{objects::Round, Coordinator};

use rocket::{http::Status, State};
use rocket_contrib::json::Json;

#[get("/ceremony")]
pub fn round_get(coordinator: State<Coordinator>) -> Result<Json<Round>, Status> {
    match coordinator.current_round() {
        Ok(round) => Ok(Json(round)),
        _ => return Err(Status::InternalServerError),
    }
}

#[cfg(test)]
mod test {
    use crate::{objects::Round, testing::prelude::*};

    #[test]
    #[serial]
    fn test_round_get() {
        let client = test_client().unwrap();

        let mut response = client.get("/ceremony").dispatch();
        let response_body = response.body_string();
        assert_eq!(Status::Ok, response.status());
        assert_eq!(Some(ContentType::JSON), response.content_type());
        assert!(response_body.is_some());

        let candidate: Round = serde_json::from_str(&response_body.unwrap()).unwrap();
        let expected = test_round_0().unwrap();
        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }
}
