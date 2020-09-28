// #[cfg(test)]
// mod tests {
//     use phase1_coordinator::{objects::Round, testing::prelude::*};
//
//     #[test]
//     #[serial]
//     fn test_e2e_basic() {
//         let client = test_client().unwrap();
//
//         let mut response = client.get("/ceremony").dispatch();
//         let response_body = response.body_string();
//         assert_eq!(Status::Ok, response.status());
//         assert_eq!(Some(ContentType::JSON), response.content_type());
//         assert!(response_body.is_some());
//
//         let candidate: Round = serde_json::from_str(&response_body.unwrap()).unwrap();
//         let expected = test_round_1_json().unwrap();
//         if candidate != expected {
//             print_diff(&expected, &candidate);
//         }
//         assert_eq!(candidate, expected);
//     }
// }
