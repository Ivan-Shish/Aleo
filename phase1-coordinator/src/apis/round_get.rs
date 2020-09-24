use crate::{objects::Round, Coordinator, Storage};

use rocket::{http::Status, State};
use rocket_contrib::json::Json;

#[get("/ceremony")]
pub fn round_get(coordinator: State<Coordinator>) -> Result<Json<Round>, Status> {
    match coordinator.get_latest_round() {
        Ok(round) => Ok(Json(round)),
        _ => return Err(Status::InternalServerError),
    }
}

#[cfg(test)]
mod test {
    use crate::{objects::Round, testing::test_server, CoordinatorError};
    use rocket::{
        http::{ContentType, Status},
        local::Client,
    };
    use serde_diff::{Apply, Diff, SerdeDiff};

    fn test_client() -> anyhow::Result<Client> {
        Ok(Client::new(test_server()?).map_err(CoordinatorError::Launch)?)
    }

    fn expected_round() -> Round {
        serde_json::from_str(include_str!("../testing/test_round.json")).unwrap()
    }

    #[test]
    fn test_round_get() {
        let client = test_client().unwrap();

        let mut response = client.get("/ceremony").dispatch();
        let response_body = response.body_string();
        assert_eq!(Status::Ok, response.status());
        assert_eq!(Some(ContentType::JSON), response.content_type());
        assert!(response_body.is_some());

        let candidate: Round = serde_json::from_str(&response_body.unwrap()).unwrap();
        let expected = expected_round();
        if candidate != expected {
            // Print the differences between the LHS and RHS JSON.
            let diff = Diff::serializable(&expected, &candidate);
            println!("{}", serde_json::to_string(&diff).unwrap());
        }
        assert_eq!(candidate, expected);
    }
}
