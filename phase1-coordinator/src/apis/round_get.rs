use crate::{objects::Round, Coordinator};

use rocket::{http::Status, State};
use rocket_contrib::json::Json;
use std::sync::Arc;

#[get("/round/<round_height>")]
pub fn round_get(coordinator: State<Arc<Coordinator>>, round_height: u64) -> Result<Json<Round>, Status> {
    match coordinator.get_round(round_height) {
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
        clear_test_transcript();

        let (client, coordinator) = test_client(&TEST_ENVIRONMENT).unwrap();
        test_initialize_to_round_1(&coordinator).unwrap();

        let mut response = client.get("/round/1").dispatch();
        let response_body = response.body_string();
        assert_eq!(Status::Ok, response.status());
        assert_eq!(Some(ContentType::JSON), response.content_type());
        assert!(response_body.is_some());

        let candidate: Round = serde_json::from_str(&response_body.unwrap()).unwrap();
        let expected = test_round_1_initial_json().unwrap();
        if candidate != expected {
            print_diff(&expected, &candidate);
        }
        assert_eq!(candidate, expected);
    }
}
