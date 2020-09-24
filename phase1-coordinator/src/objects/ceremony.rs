use crate::{
    objects::{Chunk, Round},
    CoordinatorError,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    rounds: Vec<Round>,
}

impl Transcript {
    /// Creates a new instance of `Ceremony`.
    #[inline]
    pub(crate) fn new() -> Self {
        Self { rounds: vec![] }
    }
}
