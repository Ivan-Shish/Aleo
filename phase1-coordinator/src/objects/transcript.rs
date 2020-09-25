use crate::{
    objects::{Chunk, Round},
    CoordinatorError,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    rounds: HashMap<u64, Round>,
}

impl Transcript {
    /// Creates a new instance of `Transcript`.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            rounds: HashMap::default(),
        }
    }
}
