use crate::storage::{InMemory, Storage};
use phase1::{helpers::CurveKind, ContributionMode, CurveParameters, Phase1Parameters, ProvingSystem};

use zexe_algebra::{Bls12_377, PairingEngine};

use rocket_cors::{AllowedHeaders, AllowedOrigins, Cors};

type BatchSize = usize;
type ChunkId = usize;
type ChunkSize = usize;
type Curve = CurveKind;
type Power = usize;

pub type StorageType = InMemory;

pub type Settings = (
    ContributionMode,
    ProvingSystem,
    CurveKind,
    Power,
    BatchSize,
    ChunkSize,
    ChunkId,
);

pub enum Parameters {
    AleoInner,
    AleoOuter,
    AleoUniversal,
    AleoTest,
    Simple,
}

impl Parameters {
    pub fn to_settings(&self) -> Settings {
        match self {
            Parameters::AleoInner => self.aleo_inner(),
            Parameters::AleoOuter => self.aleo_outer(),
            Parameters::AleoUniversal => self.aleo_universal(),
            Parameters::AleoTest => self.aleo_test(),
            Parameters::Simple => self.simple(),
        }
    }

    pub fn aleo_inner() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(20),
            BatchSize::from(64),
            ChunkSize::from(512),
            ChunkId::from(0),
        )
    }

    pub fn aleo_outer() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(15),
            BatchSize::from(64),
            ChunkSize::from(512),
            ChunkId::from(0),
        )
    }

    pub fn aleo_universal() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(15),
            BatchSize::from(64),
            ChunkSize::from(512),
            ChunkId::from(0),
        )
    }

    pub fn aleo_test() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(15),
            BatchSize::from(64),
            ChunkSize::from(512),
            ChunkId::from(0),
        )
    }

    pub fn simple() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(10),
            BatchSize::from(64),
            ChunkSize::from(512),
            ChunkId::from(0),
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
    /// Returns the Phase 1 parameters of the coordinator.
    pub fn phase1_parameters(&self) -> Phase1Parameters<E> {
        let (contribution_mode, proving_system, curve, power, batch_size, chunk_size, chunk_id) = match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        };

        Phase1Parameters::<_>::new(
            *contribution_mode,
            chunk_index,
            *chunk_size,
            CurveParameters::<_>::new(),
            *proving_system,
            *power,
            *batch_size,
        )
    }

    // pub fn phase1_parameters(&self) -> Phase1Parameters<E> {
    //     let (_, _, _, _, _, chunk_size, chunk_id) = match self {
    //         Environment::Test(parameters) => parameters.into_settings(),
    //         Environment::Development(parameters) => parameters.into_settings(),
    //         Environment::Production(parameters) => parameters.into_settings(),
    //     };

    /// Returns the storage system of the coordinator.
    pub fn storage<S: Storage>(&self) -> S {
        match self {
            Environment::Test(_) => InMemory,
            Environment::Development(_) => InMemory,
            Environment::Production(_) => InMemory, // TODO (howardwu): Change storage type
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

    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.address(), self.port())
    }
}
