use super::message::MessageName;

#[derive(Debug, PartialEq)]
pub enum CoordinatorMessageName {
    BandwidthChallenge,
    CpuChallenge,
    Error,
    Ping,
}

impl MessageName for CoordinatorMessageName {
    fn from_str(input: &str) -> Result<Self, String> {
        use CoordinatorMessageName::*;
        let name = match input {
            "bandwidth_challenge" => BandwidthChallenge,
            "cpu_challenge" => CpuChallenge,
            "error" => Error,
            "ping" => Ping,
            _ => return Err(format!("Unknown CoordinatorMessageName: {}", input)),
        };
        Ok(name)
    }

    fn as_bytes(&self) -> &'static [u8] {
        use CoordinatorMessageName::*;
        match self {
            BandwidthChallenge => b"bandwidth_challenge",
            CpuChallenge => b"cpu_challenge",
            Error => b"error",
            Ping => b"ping",
        }
    }
}
