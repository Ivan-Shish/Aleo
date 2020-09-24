use crate::objects::Round;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ceremony {
    rounds: Vec<Round>,
}

impl Ceremony {
    /// Creates a new instance of `Ceremony`.
    #[inline]
    pub fn new() -> Self {
        Self { rounds: vec![] }
    }

    // TODO (howardwu): Ensure verification is done and continuity is enforced.
    /// Adds a new round to the ceremony.
    #[inline]
    pub fn add_round(&mut self, round: Round) {
        self.rounds.push(round);
    }
}
