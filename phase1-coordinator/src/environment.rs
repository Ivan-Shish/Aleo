use crate::{objects::Participant, storage::InMemory};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};

use rocket_cors::{AllowedHeaders, AllowedOrigins, Cors};
use std::path::Path;

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
            ChunkSize::from(8192_usize),
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
    // /// Returns the storage system of the coordinator.
    // pub fn storage(&self) -> impl Storage {
    //     match self {
    //         Environment::Test(_) => InMemory::load(),
    //         Environment::Development(_) => InMemory::load(),
    //         Environment::Production(_) => InMemory::load(),
    //     }
    // }

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

    /// Returns an unchecked instantiation for the base URL of the coordinator.
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.address(), self.port())
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

    /// Returns the transcript directory for a given round from the coordinator.
    pub fn round_directory(&self, round_height: u64) -> String {
        match self {
            Environment::Test(_) => format!("./transcript/test/round-{}", round_height),
            Environment::Development(_) => format!("./transcript/development/round-{}", round_height),
            Environment::Production(_) => format!("./transcript/production/round-{}", round_height),
        }
    }

    /// Returns the chunk transcript directory for a given round and chunk ID from the coordinator.
    pub fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        // Create the transcript directory path.
        let path = self.round_directory(round_height);

        // Create the chunk transcript locator as `{round_directory}/{chunk_id}`.
        format!("{}/{}", path, chunk_id)
    }

    /// Returns the contribution locator for a given round, chunk ID, and contribution ID from the coordinator.
    pub fn contribution_locator(&self, round_height: u64, chunk_id: u64, contribution_id: u64) -> String {
        // Create the chunk transcript directory path.
        let path = self.chunk_directory(round_height, chunk_id);

        // If the path does not exist, attempt to initialize the directory path.
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("unable to create the chunk transcript directory");
        }

        // Create the transcript locator as `{chunk_round_directory}/{contribution_id}`.
        format!("{}/{}", path, contribution_id)
    }

    /// Returns the final round transcript locator for a given round from the coordinator.
    pub fn final_round_locator(&self, round_height: u64) -> String {
        // Create the transcript directory path.
        let path = self.round_directory(round_height);

        // If the path does not exist, attempt to initialize the directory path.
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("unable to create the chunk transcript directory");
        }

        // Create the final round transcript locator located at `{round_directory}/contribution`.
        format!("{}/contribution", path)
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
