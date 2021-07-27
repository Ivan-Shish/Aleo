use super::message::MessageName;

#[derive(Debug, PartialEq)]
pub enum ContributorMessageName {
    BandwidthChallenge,
    CpuChallenge,
    Error,
    Pong,
}

impl MessageName for ContributorMessageName {
    fn from_str(input: &str) -> Result<Self, String> {
        use ContributorMessageName::*;
        let name = match input {
            "bandwidth_challenge" => BandwidthChallenge,
            "cpu_challenge" => CpuChallenge,
            "error" => Error,
            "pong" => Pong,
            _ => return Err(format!("Unknown ContributorMessageName: {}", input)),
        };
        Ok(name)
    }

    fn as_bytes(&self) -> &'static [u8] {
        use ContributorMessageName::*;
        match self {
            BandwidthChallenge => b"bandwidth_challenge",
            CpuChallenge => b"cpu_challenge",
            Error => b"error",
            Pong => b"pong",
        }
    }
}
