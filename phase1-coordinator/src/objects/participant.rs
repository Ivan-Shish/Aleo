use rocket::{
    data::{self, FromDataSimple},
    http::{ContentType, Status},
    Data,
    Outcome,
    Outcome::*,
    Request,
};
use serde::{Deserialize, Serialize};
use serde_diff::SerdeDiff;
use std::{fmt, io::Read};

pub type ContributorId = String;
pub type VerifierId = String;

// Always use a limit to prevent DoS attacks.
const DATA_LIMIT: u64 = 256;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, SerdeDiff)]
pub enum Participant {
    Contributor(ContributorId),
    Verifier(VerifierId),
}

impl Participant {
    /// Creates a new instance of `Participant`.
    pub fn new_contributor(participant: String) -> Self {
        Participant::Contributor(participant)
    }

    /// Returns `true` if the participant is a contributor.
    /// Otherwise, returns `false`.
    pub fn is_contributor(&self) -> bool {
        match self {
            Participant::Contributor(_) => true,
            Participant::Verifier(_) => false,
        }
    }

    /// Returns `true` if the participant is a verifier.
    /// Otherwise, returns `false`.
    pub fn is_verifier(&self) -> bool {
        !self.is_contributor()
    }
}

impl FromDataSimple for Participant {
    type Error = String;

    fn from_data(req: &Request, data: Data) -> data::Outcome<Self, String> {
        // Ensure the content type is correct before opening the data.
        if req.content_type() != Some(&ContentType::new("application", "x-participant")) {
            return Outcome::Forward(data);
        }

        // Read the data as a participant.
        let mut participant = String::new();
        if let Err(e) = data.open().take(DATA_LIMIT).read_to_string(&mut participant) {
            return Failure((Status::InternalServerError, format!("{:?}", e)));
        }

        // By default, we will always set this to a contributor.
        Success(Participant::new_contributor(participant))
    }
}

impl fmt::Display for Participant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Participant::Contributor(contributor_id) => write!(f, "{}", contributor_id),
            Participant::Verifier(verifier_id) => write!(f, "{}", verifier_id),
        }
    }
}
