use serde::{
    de::{self, Deserializer},
    Deserialize,
    Serialize,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ceremony {
    rounds: Vec<Round>,
}
