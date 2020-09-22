use rayon::prelude::*;
use serde::{
    de::{self, Deserializer},
    Deserialize,
    Serialize,
};
use serde_aux::prelude::*;
use std::{fmt::Display, str::FromStr};
use url::{ParseError, Url};
use url_serde;

// TODO (howardwu): Change this to match.
// #[derive(Debug, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub struct Contributor {
//     #[serde(flatten)]
//     address: String
// }

// TODO (howardwu): Change this to match.
// #[derive(Debug, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub struct Verifier {
//     address: String
// }

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Contribution {
    contributor_id: Option<String>,
    #[serde(with = "url_serde")]
    contributed_location: Option<Url>,
    verifier_id: Option<String>,
    #[serde(with = "url_serde")]
    verified_location: Option<Url>,
    verified: bool,
}
