use crate::CoordinatorError;

use serde::{Deserialize, Serialize};
use url::Url;
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

impl Contribution {
    /// Creates a new contributor instance of `Contribution`.
    #[inline]
    pub(crate) fn new_contributor(
        chunk_id: u64,
        contribution_id: u64,
        contributor_id: String,
        contributed_base_url: &str,
    ) -> Result<Self, CoordinatorError> {
        Ok(Self {
            contributor_id: Some(contributor_id),
            contributed_location: Some(
                format!(
                    "{}/chunks/{}/contribution/{}",
                    contributed_base_url, chunk_id, contribution_id
                )
                .parse()?,
            ),
            verifier_id: None,
            verified_location: None,
            verified: false,
        })
    }

    /// Creates a new verifier instance of `Contribution`.
    #[inline]
    pub(crate) fn new_verifier(
        chunk_id: u64,
        contribution_id: u64,
        verifier_id: String,
        verified_base_url: &str,
    ) -> Result<Self, CoordinatorError> {
        Ok(Self {
            contributor_id: None,
            contributed_location: None,
            verifier_id: Some(verifier_id),
            verified_location: Some(
                format!(
                    "{}/chunks/{}/contribution/{}",
                    verified_base_url, chunk_id, contribution_id
                )
                .parse()?,
            ),
            verified: false,
        })
    }

    /// Returns a reference to the contributor ID, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_contributor_id(&self) -> &Option<String> {
        &self.contributor_id
    }

    /// Returns a reference to the contributor location, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_contributed_location(&self) -> &Option<Url> {
        &self.contributed_location
    }

    /// Returns a reference to the verifier ID, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_verifier_id(&self) -> &Option<String> {
        &self.verifier_id
    }

    /// Returns a reference to the verifier location, if it exists.
    /// Otherwise returns `None`.
    #[inline]
    pub fn get_verified_location(&self) -> &Option<Url> {
        &self.verified_location
    }

    /// Returns `true` if the contribution has been verified.
    /// Otherwise returns `false`.
    #[inline]
    pub fn is_verified(&self) -> bool {
        self.verified
    }
}
