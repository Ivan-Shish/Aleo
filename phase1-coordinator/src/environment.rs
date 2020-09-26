use crate::storage::{InMemory, Storage};
use phase1::{helpers::CurveKind, ContributionMode, CurveParameters, Phase1Parameters, ProvingSystem};
// use phase1_coordinator_derives::phase1_parameters;

use zexe_algebra::{Bls12_377, PairingEngine, BW6_761};

use rocket_cors::{AllowedHeaders, AllowedOrigins, Cors};
use url::Url;

type BatchSize = usize;
type ChunkId = usize;
type ChunkSize = usize;
type Curve = CurveKind;
type Power = usize;

pub type StorageType = InMemory;

pub type Settings = (ContributionMode, ProvingSystem, Curve, Power, BatchSize, ChunkSize);

#[derive(Debug, Copy, Clone)]
pub enum Parameters {
    AleoInner,
    AleoOuter,
    AleoUniversal,
    AleoTest,
    Simple,
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
        }
    }

    fn aleo_inner() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(20_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(512_usize),
        )
    }

    fn aleo_outer() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(15_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(512_usize),
        )
    }

    fn aleo_universal() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Marlin,
            CurveKind::Bls12_377,
            Power::from(15_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(512_usize),
        )
    }

    fn aleo_test() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(15_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(512_usize),
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

#[derive(Debug, Copy, Clone)]
pub enum Environment {
    Test(Parameters),
    Development(Parameters),
    Production(Parameters),
}

impl Environment {
    // TODO (howardwu): Change storage type
    /// Returns the storage system of the coordinator.
    pub fn storage(&self) -> impl Storage {
        match self {
            Environment::Test(_) => InMemory::load(),
            Environment::Development(_) => InMemory::load(),
            Environment::Production(_) => InMemory::load(),
        }
    }

    /// Returns the appropriate number of chunks for the coordinator
    /// to run given a proof system, power and chunk size.
    pub fn number_of_chunks(&self) -> u64 {
        let (_, proving_system, _, power, _, chunk_size) = match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        };

        match proving_system {
            ProvingSystem::Groth16 => u64::pow(2, power as u32) / chunk_size as u64,
            ProvingSystem::Marlin => u64::pow(2, power as u32) / chunk_size as u64,
        }
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

    /// Returns the parameter settings of the coordinater.
    pub fn to_settings(&self) -> Settings {
        match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        }
    }
}
