use crate::{
    objects::Participant,
    storage::{Disk, Storage},
};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};
use setup_utils::{CheckForCorrectness, UseCompression};

use url::Url;

type BatchSize = usize;
type ChunkSize = usize;
type Curve = CurveKind;
type NumberOfChunks = usize;
type Power = usize;

pub type Settings = (ContributionMode, ProvingSystem, Curve, Power, BatchSize, ChunkSize);

#[derive(Debug, Clone)]
pub enum Parameters {
    AleoInner,
    AleoOuter,
    AleoUniversal,
    AleoTest3Chunks,
    AleoTest8Chunks,
    AleoTestChunks(NumberOfChunks),
    AleoTestCustom(NumberOfChunks, Power, BatchSize),
    Custom(Settings),
}

impl Parameters {
    /// Returns the corresponding settings for each parameter type.
    fn to_settings(&self) -> Settings {
        match self {
            Parameters::AleoInner => Self::aleo_inner(),
            Parameters::AleoOuter => Self::aleo_outer(),
            Parameters::AleoUniversal => Self::aleo_universal(),
            Parameters::AleoTest3Chunks => Self::aleo_test_3_chunks(),
            Parameters::AleoTest8Chunks => Self::aleo_test_8_chunks(),
            Parameters::AleoTestChunks(number_of_chunks) => Self::aleo_test_chunks(number_of_chunks),
            Parameters::AleoTestCustom(number_of_chunks, power, batch_size) => {
                Self::aleo_test_custom(number_of_chunks, power, batch_size)
            }
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

    fn aleo_test_3_chunks() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(8_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(172_usize),
        )
    }

    fn aleo_test_8_chunks() -> Settings {
        (
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(14_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(4096_usize),
        )
    }

    fn aleo_test_chunks(number_of_chunks: &NumberOfChunks) -> Settings {
        let proving_system = ProvingSystem::Groth16;
        let power = 14_usize;
        let batch_size = 128_usize;
        (
            ContributionMode::Chunked,
            proving_system,
            CurveKind::Bls12_377,
            Power::from(power),
            BatchSize::from(batch_size),
            chunk_size!(number_of_chunks, proving_system, power),
        )
    }

    fn aleo_test_custom(number_of_chunks: &NumberOfChunks, power: &Power, batch_size: &BatchSize) -> Settings {
        let proving_system = ProvingSystem::Groth16;
        (
            ContributionMode::Chunked,
            proving_system,
            CurveKind::Bls12_377,
            *power,
            *batch_size,
            chunk_size!(number_of_chunks, proving_system, power),
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
    /// Returns the parameter settings of the coordinator.
    pub fn to_settings(&self) -> Settings {
        match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        }
    }

    ///
    /// Returns the appropriate number of chunks for the coordinator
    /// to run given a proof system, power and chunk size.
    ///
    pub fn number_of_chunks(&self) -> u64 {
        let (_, proving_system, _, power, _, chunk_size) = match self {
            Environment::Test(parameters) => parameters.to_settings(),
            Environment::Development(parameters) => parameters.to_settings(),
            Environment::Production(parameters) => parameters.to_settings(),
        };
        (total_size_in_g1!(proving_system, power) + chunk_size as u64 - 1) / chunk_size as u64
    }

    ///
    /// Returns the minimum number of contributors permitted to
    /// participate in a round.
    ///
    pub fn minimum_contributors_per_round(&self) -> usize {
        match self {
            Environment::Test(_) => 1,
            Environment::Development(_) => 1,
            Environment::Production(_) => 1,
        }
    }

    ///
    /// Returns the maximum number of contributors permitted to
    /// participate in a round.
    ///
    pub fn maximum_contributors_per_round(&self) -> usize {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the minimum number of verifiers permitted to
    /// participate in a round.
    ///
    pub fn minimum_verifiers_per_round(&self) -> usize {
        match self {
            Environment::Test(_) => 1,
            Environment::Development(_) => 1,
            Environment::Production(_) => 1,
        }
    }

    ///
    /// Returns the maximum number of verifiers permitted to
    /// participate in a round.
    ///
    pub fn maximum_verifiers_per_round(&self) -> usize {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the number of chunks a contributor is
    /// authorized to lock in tandem at any point during a round.
    ///
    pub fn contributor_lock_chunk_limit(&self) -> usize {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the number of chunks a verifier is
    /// authorized to lock in tandem at any point during a round.
    ///
    pub fn verifier_lock_chunk_limit(&self) -> usize {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the number of minutes the coordinator tolerates
    /// before assuming a contributor has disconnected.
    ///
    pub fn contributor_timeout_in_minutes(&self) -> u16 {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the number of minutes the coordinator tolerates
    /// before assuming a verifier has disconnected.
    ///
    pub fn verifier_timeout_in_minutes(&self) -> u16 {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the number of times the coordinator tolerates
    /// a dropped participant before banning them from future rounds.
    ///
    pub fn participant_ban_threshold(&self) -> u16 {
        match self {
            Environment::Test(_) => 5,
            Environment::Development(_) => 5,
            Environment::Production(_) => 5,
        }
    }

    ///
    /// Returns the compressed input preference of the coordinator.
    ///
    /// By default, the coordinator returns `false` to minimize time
    /// spent by contributors on decompressing inputs.
    ///
    pub const fn compressed_inputs(&self) -> UseCompression {
        match self {
            Environment::Test(_) => UseCompression::No,
            Environment::Development(_) => UseCompression::No,
            Environment::Production(_) => UseCompression::No,
        }
    }

    ///
    /// Returns the compressed output preference of the coordinator.
    ///
    /// By default, the coordinator returns `true` to minimize time
    /// spent by the coordinator and contributors on uploading chunks.
    ///
    pub const fn compressed_outputs(&self) -> UseCompression {
        match self {
            Environment::Test(_) => UseCompression::Yes,
            Environment::Development(_) => UseCompression::Yes,
            Environment::Production(_) => UseCompression::Yes,
        }
    }

    ///
    /// Returns the input correctness check preference of the coordinator.
    ///
    /// By default, the coordinator returns `false` to minimize time
    /// spent by the contributors on reading chunks.
    ///
    pub fn check_input_for_correctness(&self) -> CheckForCorrectness {
        match self {
            Environment::Test(_) => CheckForCorrectness::No,
            Environment::Development(_) => CheckForCorrectness::No,
            Environment::Production(_) => CheckForCorrectness::No,
        }
    }

    /// Returns the base URL for the coordinator.
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
            Environment::Production(_) => "1.2.3.4",
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

    /// Returns the base directory for the local locator of this coordinator.
    pub const fn local_base_directory(&self) -> &str {
        match self {
            Environment::Test(_) => "./transcript/test",
            Environment::Development(_) => "./transcript/development",
            Environment::Production(_) => "./transcript",
        }
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

    /// Returns the version number of the coordinator.
    pub const fn version(&self) -> u64 {
        match self {
            Environment::Test(_) => 1,
            Environment::Development(_) => 1,
            Environment::Production(_) => 1,
        }
    }

    /// Returns the storage system of the coordinator.
    pub(crate) fn storage(&self) -> anyhow::Result<Box<dyn Storage>> {
        Ok(storage!(self, Disk, Disk, Disk))
    }
}

#[cfg(test)]
mod tests {
    use crate::environment::*;

    #[test]
    fn test_aleo_test_3_chunks() {
        let parameters = Parameters::AleoTest3Chunks;
        let (_, _, _, power, _, _) = parameters.to_settings();
        assert_eq!(Power::from(8_usize), power);
        assert_eq!(3, Environment::Test(parameters).number_of_chunks());
    }

    #[test]
    fn test_aleo_test_8_chunks() {
        let parameters = Parameters::AleoTest8Chunks;
        let (_, _, _, power, _, _) = parameters.to_settings();
        assert_eq!(Power::from(14_usize), power);
        assert_eq!(8, Environment::Test(parameters).number_of_chunks());
    }

    #[test]
    fn test_custom_chunk_3() {
        let number_of_chunks = 3;

        let parameters = Parameters::AleoTestChunks(number_of_chunks);
        let (_, _, _, power, _, chunk_size) = parameters.to_settings();
        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(10923_usize), chunk_size);
        assert_eq!(
            number_of_chunks as u64,
            Environment::Test(parameters).number_of_chunks()
        );
    }

    #[test]
    fn test_custom_chunk_8() {
        let number_of_chunks = 8;

        let parameters = Parameters::AleoTestChunks(number_of_chunks);
        let (_, _, _, power, _, chunk_size) = parameters.to_settings();
        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(4096_usize), chunk_size);
        assert_eq!(
            number_of_chunks as u64,
            Environment::Test(parameters).number_of_chunks()
        );
    }

    #[test]
    fn test_custom_chunk_20() {
        let number_of_chunks = 20;

        let parameters = Parameters::AleoTestChunks(number_of_chunks);
        let (_, _, _, power, _, chunk_size) = parameters.to_settings();
        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(1639_usize), chunk_size);
        assert_eq!(
            number_of_chunks as u64,
            Environment::Test(parameters).number_of_chunks()
        );
    }
}
