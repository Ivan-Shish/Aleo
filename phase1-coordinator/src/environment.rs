use crate::{
    objects::Participant,
    storage::{InMemory, InMemory2, Storage},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};

use rocket_cors::{AllowedHeaders, AllowedOrigins, Cors};
use url::Url;

type BatchSize = usize;
type ChunkSize = usize;
type Curve = CurveKind;
type Power = usize;

pub type StorageType = InMemory;

pub type Settings = (ContributionMode, ProvingSystem, Curve, Power, BatchSize, ChunkSize);

#[derive(Debug, Clone)]
pub enum Parameters {
    AleoInner,
    AleoOuter,
    AleoUniversal,
    AleoTest,
    AleoTest20,
    Simple,
    Custom(Settings),
}

impl Parameters {
    /// Returns the corresponding settings for each parameter type.
    fn to_settings(&self) -> Settings {
        match self {
            Parameters::AleoInner => Self::aleo_inner(),
            Parameters::AleoOuter => Self::aleo_outer(),
            Parameters::AleoUniversal => Self::aleo_universal(),
            Parameters::AleoTest => Self::aleo_test(),
            Parameters::AleoTest20 => Self::aleo_test_20(),
            Parameters::Simple => Self::simple(),
            Parameters::Custom(settings) => settings.clone(),
        }
    }

    fn aleo_inner() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(20_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(2048_usize),
        )
    }

    fn aleo_outer() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(21_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(8192_usize),
        )
    }

    fn aleo_universal() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Marlin,
            CurveKind::Bls12_377,
            Power::from(30_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(8192_usize),
        )
    }

    fn aleo_test() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(14_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(4095_usize),
        )
    }

    fn aleo_test_20() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(14_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(1637_usize),
        )
    }

    fn simple() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(10_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(512_usize),
        )
    }
}

#[derive(Debug, Clone)]
pub enum Environment {
    Test(Parameters),
    Development(Parameters),
    Production(Parameters),
}

impl Environment {
    // // TODO (howardwu): Change storage type
    /// Returns the storage system of the coordinator.
    pub fn storage(&self) -> Result<Box<dyn Storage>, CoordinatorError> {
        Ok(storage!(self, InMemory, InMemory2, InMemory2))
    }

    /// Returns the appropriate number of chunks for the coordinator
    /// to run given a proof system, power and chunk size.
    pub fn number_of_chunks(&self) -> u64 {
        let (_, proving_system, _, power, _, chunk_size) = match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        };

        return match proving_system {
            ProvingSystem::Groth16 => ((1 << (power + 1)) - 1) / chunk_size,
            ProvingSystem::Marlin => (1 << power) / chunk_size,
        } as u64;
    }

    ///
    /// Returns the compressed input preference of the coordinator.
    ///
    /// By default, the coordinator returns `false` to minimize time
    /// spent by contributors on decompressing inputs.
    ///
    pub fn compressed_inputs(&self) -> bool {
        match self {
            Environment::Test(_) => false,
            Environment::Development(_) => false,
            Environment::Production(_) => false,
        }
    }

    ///
    /// Returns the compressed output preference of the coordinator.
    ///
    /// By default, the coordinator returns `true` to minimize time
    /// spent by the coordinator and contributors on uploading chunks.
    ///
    pub fn compressed_outputs(&self) -> bool {
        match self {
            Environment::Test(_) => true,
            Environment::Development(_) => true,
            Environment::Production(_) => true,
        }
    }

    /// Returns the version number of the coordinator.
    pub const fn version(&self) -> u64 {
        match self {
            Environment::Test(_) => 1,
            Environment::Development(_) => 1,
            Environment::Production(_) => 1,
        }
    }

    /// Returns an instantiation of the base URL for the coordinator.
    pub fn base_url(&self) -> Url {
        format!("http://{}:{}", self.address(), self.port())
            .parse()
            .expect("Unable to parse base URL")
    }

    /// Returns the network address of the coordinator.
    pub const fn address(&self) -> &str {
        match self {
            Environment::Test(_) => "localhost",
            Environment::Development(_) => "0.0.0.0",
            Environment::Production(_) => "167.71.156.62",
        }
    }

    /// Returns the network port of the coordinator.
    pub const fn port(&self) -> u16 {
        match self {
            Environment::Test(_) => 8080,
            Environment::Development(_) => 8080,
            Environment::Production(_) => 8080,
        }
    }

    /// Returns the CORS policy of the server.
    pub fn cors(&self) -> Cors {
        let allowed_origins = match self {
            Environment::Test(_) => AllowedOrigins::all(),
            Environment::Development(_) => AllowedOrigins::all(),
            Environment::Production(_) => AllowedOrigins::all(),
        };

        let allowed_headers = match self {
            Environment::Test(_) => AllowedHeaders::all(),
            Environment::Development(_) => AllowedHeaders::all(),
            Environment::Production(_) => AllowedHeaders::all(),
        };

        Cors {
            allowed_origins,
            allowed_headers,
            allow_credentials: true,
            ..Default::default()
        }
    }

    /// Returns the round directory for a given round height.
    pub fn round_directory(&self, round_height: u64) -> String {
        round_directory!(self, Local, Remote, Remote, round_height)
    }

    /// Returns `true` if the round directory exists for a given round height.
    pub fn round_directory_exists(&self, round_height: u64) -> bool {
        round_directory_exists!(self, Local, Remote, Remote, round_height)
    }

    /// Resets the round directory for a given round height, if permitted.
    pub fn round_directory_reset(&self, round_height: u64) {
        round_directory_reset!(self, Local, Remote, Remote, round_height)
    }

    /// Returns the chunk directory for a given round height and chunk ID.
    pub fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        chunk_directory!(self, Local, Remote, Remote, round_height, chunk_id)
    }

    /// Returns `true` if the chunk directory exists for a given round height and chunk ID.
    pub fn chunk_directory_exists(&self, round_height: u64, chunk_id: u64) -> bool {
        chunk_directory_exists!(self, Local, Remote, Remote, round_height, chunk_id)
    }

    /// Returns the contribution locator for a given round, chunk ID, and contribution ID.
    pub fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64) -> String {
        contribution_locator!(self, Local, Remote, Remote, round_height, chunk_id, contribution_id)
    }

    /// Returns `true` if the contribution locator exists for a given round, chunk ID, and contribution ID.
    pub fn contribution_locator_exists(&self, round_height: u64, chunk_id: u64, contribution_id: u64) -> bool {
        contribution_locator_exists!(self, Local, Remote, Remote, round_height, chunk_id, contribution_id)
    }

    /// Returns the round locator for a given round height.
    pub fn round_locator(&self, round_height: u64) -> String {
        round_locator!(self, Local, Remote, Remote, round_height)
    }

    /// Returns `true` if the round locator exists for a given round height.
    pub fn round_locator_exists(&self, round_height: u64) -> bool {
        round_locator_exists!(self, Local, Remote, Remote, round_height)
    }

    ///
    /// Returns the contributor managed by the coordinator.
    ///
    /// The primary purpose of this is to establish an identity for the coordinator
    /// when running initialization of each round.
    ///
    /// This can also be purposed for completing contributions of participants
    /// who may have dropped off and handed over control of their session.
    ///
    pub fn coordinator_contributor(&self) -> Participant {
        match self {
            Environment::Test(_) => Participant::Contributor(format!("test-coordinator-contributor")),
            Environment::Development(_) => Participant::Contributor(format!("development-coordinator-contributor")),
            Environment::Production(_) => Participant::Contributor(format!("production-coordinator-contributor")),
        }
    }

    /// Returns a verifier managed by the coordinator.
    pub fn coordinator_verifier(&self) -> Participant {
        match self {
            Environment::Test(_) => Participant::Verifier(format!("test-coordinator-verifier")),
            Environment::Development(_) => Participant::Verifier(format!("development-coordinator-verifier")),
            Environment::Production(_) => Participant::Verifier(format!("production-coordinator-verifier")),
        }
    }

    /// Returns the parameter settings of the coordinator.
    pub fn to_settings(&self) -> Settings {
        match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        }
    }
}
