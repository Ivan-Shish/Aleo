use chrono::Utc;

#[get("/timestamp")]
pub fn timestamp_get() -> String {
    format!("{}", Utc::now())
}

#[cfg(test)]
mod test {
    use crate::testing::prelude::*;

    use chrono::{DateTime, Utc};

    #[test]
    #[serial]
    fn test_timestamp_get() {
        let (client, _) = test_client(&TEST_ENVIRONMENT).unwrap();

        let mut response = client.get("/timestamp").dispatch();
        assert_eq!(Status::Ok, response.status());
        assert!(response.body_string().unwrap().parse::<DateTime<Utc>>().is_ok());
    }
}
