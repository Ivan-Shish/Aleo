use rocket::{
    data::{self, FromDataSimple},
    http::{ContentType, Status},
    Data,
    Outcome,
    Outcome::*,
    Request,
};
use serde::{de::Deserializer, Deserialize, Serialize};
use serde_diff::SerdeDiff;
use std::{
    fmt::{self},
    io::Read,
};

pub type ContributorId = String;
pub type VerifierId = String;

// Always use a limit to prevent DoS attacks.
const DATA_LIMIT: u64 = 256;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, SerdeDiff)]
#[serde(untagged)]
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

/// Deserializes a contributor from a string.
pub fn deserialize_contributor_from_string<'de, D>(deserializer: D) -> Result<Participant, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        Contributor(String),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::Contributor(id) => Ok(Participant::Contributor(id)),
    }
}

/// Deserializes a optional contributor from a string.
pub fn deserialize_optional_contributor_from_string<'de, D>(deserializer: D) -> Result<Option<Participant>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        Contributor(String),
        MaybeContributor(Option<String>),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::Contributor(id) => Ok(Some(Participant::Contributor(id))),
        ParticipantString::MaybeContributor(id) => match id {
            Some(id) => Ok(Some(Participant::Contributor(id))),
            None => Ok(None),
        },
    }
}

/// Deserializes a list of contributors from a list of strings.
pub fn deserialize_contributors_from_strings<'de, D>(deserializer: D) -> Result<Vec<Participant>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        List(Vec<String>),
        // Monolith(String),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::List(ids) => {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Participant::Contributor(id))
            }
            Ok(result)
        } // ParticipantString::Monolith(ids) => {
          //     let ids: Vec<String> = serde_json::from_str(&ids).unwrap();
          //
          //     let mut result = Vec::with_capacity(ids.len());
          //     for id in ids {
          //         result.push(Participant::Contributor(id))
          //     }
          //     Ok(result)
          // }
    }
}

/// Deserializes a verifier from a string.
pub fn deserialize_verifier_from_string<'de, D>(deserializer: D) -> Result<Participant, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        Verifier(String),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::Verifier(id) => Ok(Participant::Verifier(id)),
    }
}

/// Deserializes a optional verifier from a string.
pub fn deserialize_optional_verifier_from_string<'de, D>(deserializer: D) -> Result<Option<Participant>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        Verifier(String),
        MaybeVerifier(Option<String>),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::Verifier(id) => Ok(Some(Participant::Verifier(id))),
        ParticipantString::MaybeVerifier(id) => match id {
            Some(id) => Ok(Some(Participant::Verifier(id))),
            None => Ok(None),
        },
    }
}

/// Deserializes a list of verifiers from a list of strings.
pub fn deserialize_verifiers_from_strings<'de, D>(deserializer: D) -> Result<Vec<Participant>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        List(Vec<String>),
        // Monolith(String),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::List(ids) => {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Participant::Verifier(id))
            }
            Ok(result)
        } // ParticipantString::Monolith(ids) => {
          //     let ids: Vec<String> = serde_json::from_str(&ids).unwrap();
          //
          //     let mut result = Vec::with_capacity(ids.len());
          //     for id in ids {
          //         result.push(Participant::Verifier(id))
          //     }
          //     Ok(result)
          // }
    }
}
