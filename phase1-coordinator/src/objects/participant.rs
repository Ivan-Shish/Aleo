use serde::{
    de::{Deserializer, Error},
    Deserialize,
    Serialize,
    Serializer,
};
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

impl fmt::Display for Participant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Participant::Contributor(contributor_id) => write!(f, "{}", contributor_id),
            Participant::Verifier(verifier_id) => write!(f, "{}", verifier_id),
        }
    }
}

/// Serializes a optional participant to an optional string.
pub fn serialize_optional_participant_to_optional_string<S>(
    participant: &Option<Participant>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match participant {
        Some(participant) => serializer.serialize_some(&match participant {
            Participant::Contributor(id) => Some(("contributor", id.as_str())),
            Participant::Verifier(id) => Some(("verifier", id.as_str())),
        }),
        None => serializer.serialize_none(),
    }
}

/// Deserializes a optional participant to an optional string.
pub fn deserialize_optional_participant_to_optional_string<'de, D>(
    deserializer: D,
) -> Result<Option<Participant>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        OptionalParticipant(Option<(String, String)>),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::OptionalParticipant(participant) => match participant {
            Some((variant, id)) => match variant.as_str() {
                "contributor" => Ok(Some(Participant::Contributor(id))),
                "verifier" => Ok(Some(Participant::Verifier(id))),
                _ => Ok(None),
            },
            None => Ok(None),
        },
    }
}

/// Serializes a participant to a string.
pub fn serialize_participant_to_string<S>(participant: &Participant, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_some(&match participant {
        Participant::Contributor(id) => ("contributor", id.as_str()),
        Participant::Verifier(id) => ("verifier", id.as_str()),
    })
}

/// Deserializes a participant to a string.
pub fn deserialize_participant_to_string<'de, D>(deserializer: D) -> Result<Participant, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ParticipantString {
        Participant((String, String)),
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::Participant(participant) => match participant {
            (variant, id) => match variant.as_str() {
                "contributor" => Ok(Participant::Contributor(id)),
                "verifier" => Ok(Participant::Verifier(id)),
                _ => Err(D::Error::custom("invalid participant type")),
            },
        },
    }
}

/// Deserializes a contributor from a string.
#[allow(dead_code)]
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
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::List(ids) => {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Participant::Contributor(id))
            }
            Ok(result)
        }
    }
}

/// Deserializes a verifier from a string.
#[allow(dead_code)]
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
    }

    match ParticipantString::deserialize(deserializer)? {
        ParticipantString::List(ids) => {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Participant::Verifier(id))
            }
            Ok(result)
        }
    }
}
