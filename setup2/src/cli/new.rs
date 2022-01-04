use phase2::parameters::MPCParameters;
use setup_utils::{log_2, CheckForCorrectness, UseCompression};
use snarkvm_algorithms::{CRH, SNARK, SRS};
use snarkvm_curves::PairingEngine;
use snarkvm_dpc::{
    network::testnet2::Testnet2,
    traits::Network,
    AleoLocator,
    AleoObject,
    Execution,
    Function,
    InnerCircuit,
    Noop,
    NoopPrivateVariables,
    OuterCircuit,
    ProgramPrivateVariables,
    ProgramPublicVariables,
};
use snarkvm_fields::Field;
use snarkvm_r1cs::{ConstraintCounter, ConstraintSynthesizer};
use snarkvm_utilities::CanonicalSerialize;
use tracing::{debug, info};

use gumdrop::Options;
use memmap::MmapOptions;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};
use setup_utils::calculate_hash;
use std::{fs::OpenOptions, io::Write};

type AleoInner = <Testnet2 as Network>::InnerCurve;
type AleoOuter = <Testnet2 as Network>::OuterCurve;

const COMPRESSION: UseCompression = UseCompression::No;

pub const SEED_LENGTH: usize = 32;
pub type Seed = [u8; SEED_LENGTH];

#[derive(Debug, Clone)]
pub enum CurveKind {
    Bls12_377,
    BW6,
}

pub fn curve_from_str(src: &str) -> std::result::Result<CurveKind, String> {
    let curve = match src.to_lowercase().as_str() {
        "bls12_377" => CurveKind::Bls12_377,
        "bw6" => CurveKind::BW6,
        _ => return Err("unsupported curve.".to_string()),
    };
    Ok(curve)
}

#[derive(Clone, PartialEq, Eq, Debug, Copy, Serialize, Deserialize)]
pub enum ContributionMode {
    Full,
    Chunked,
}

pub fn contribution_mode_from_str(src: &str) -> Result<ContributionMode, String> {
    let mode = match src.to_lowercase().as_str() {
        "full" => ContributionMode::Full,
        "chunked" => ContributionMode::Chunked,
        _ => return Err("unsupported contribution mode. Currently supported: full, chunked".to_string()),
    };
    Ok(mode)
}

#[derive(Debug, Options, Clone)]
pub struct NewOpts {
    help: bool,
    #[options(help = "the total number of coefficients (in powers of 2) which were created after processing phase 1")]
    pub phase1_size: u32,
    #[options(help = "the challenge file name to be created", default = "challenge")]
    pub output: String,

    #[options(
        help = "the elliptic curve to use",
        default = "bls12_377",
        parse(try_from_str = "curve_from_str")
    )]
    pub curve_type: CurveKind,

    #[options(
        help = "the contribution mode",
        default = "chunked",
        parse(try_from_str = "contribution_mode_from_str")
    )]
    pub contribution_mode: ContributionMode,

    #[options(help = "the chunk size")]
    pub chunk_size: usize,

    #[options(help = "the size of batches to process", default = "256")]
    pub batch_size: usize,

    #[options(help = "setup the inner or the outer circuit?", default = "true")]
    pub is_inner: String,

    #[options(help = "the provided challenge file", default = "challenge")]
    pub challenge_fname: String,
    #[options(help = "the new challenge file hash", default = "challenge.verified.hash")]
    pub challenge_hash_fname: String,
    #[options(help = "the provided response file which will be verified", default = "response")]
    pub response_fname: String,
    #[options(
        help = "the new challenge file which will be generated in response",
        default = "new_challenge"
    )]
    pub new_challenge_fname: String,
    #[options(help = "phase 1 file name", default = "phase1")]
    pub phase1_fname: String,
    #[options(help = "phase 1 powers")]
    pub phase1_powers: usize,
    #[options(help = "number of validators")]
    pub num_validators: usize,
    #[options(help = "number of epochs")]
    pub num_epochs: usize,
}

pub fn new(opt: &NewOpts) -> anyhow::Result<()> {
    if opt.is_inner == "true" {
        let circuit = InnerCircuit::<Testnet2>::blank();
        generate_params_chunked::<AleoInner, _>(opt, circuit)
    } else {
        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
        let rng = &mut ChaChaRng::from_seed(seed);

        // Generate inner circuit parameters and proof for verification in the outer circuit.
        let inner_circuit = InnerCircuit::<Testnet2>::blank();
        let (inner_proving_key, inner_verifying_key) =
            <Testnet2 as Network>::InnerSNARK::setup(&inner_circuit, &mut SRS::CircuitSpecific(rng)).unwrap();

        let inner_proof = AleoObject::from(
            <Testnet2 as Network>::InnerSNARK::prove(&inner_proving_key, &inner_circuit, rng).unwrap(),
        );

        let transition_id = AleoLocator::from(<<Testnet2 as Network>::TransitionIDCRH as CRH>::Output::default());
        let noop_execution = Execution {
            program_id: *Testnet2::noop_program_id(),
            program_path: Testnet2::noop_program_path().clone(),
            verifying_key: Testnet2::noop_circuit_verifying_key().clone(),
            proof: Noop::<Testnet2>::new()
                .execute(
                    ProgramPublicVariables::new(transition_id),
                    &NoopPrivateVariables::<Testnet2>::new_blank().unwrap(),
                )
                .unwrap(),
        };
        let outer_circuit = OuterCircuit::<Testnet2>::blank(inner_verifying_key, inner_proof, noop_execution);

        generate_params_chunked::<AleoOuter, _>(opt, outer_circuit)
    }
}

/// Returns the number of powers required for the Phase 2 ceremony
/// = log2(aux + inputs + constraints)
fn ceremony_size<F: Field, C: Clone + ConstraintSynthesizer<F>>(circuit: &C) -> usize {
    let mut counter = ConstraintCounter {
        num_public_variables: 0,
        num_private_variables: 0,
        num_constraints: 0,
    };
    circuit
        .clone()
        .generate_constraints(&mut counter)
        .expect("could not calculate number of required constraints");
    let phase2_size = std::cmp::max(
        counter.num_constraints,
        counter.num_private_variables + counter.num_public_variables + 1,
    );
    debug!("Expected phase2_size: {}", phase2_size);
    let power = log_2(phase2_size) as u32;

    // get the nearest power of 2
    if phase2_size < 2usize.pow(power) {
        2usize.pow(power + 1)
    } else {
        phase2_size
    }
}

pub fn generate_params_chunked<E, C>(opt: &NewOpts, circuit: C) -> anyhow::Result<()>
where
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    let phase1_transcript = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&opt.phase1_fname)
        .expect("could not read phase 1 transcript file");
    let mut phase1_transcript = unsafe {
        MmapOptions::new()
            .map_mut(&phase1_transcript)
            .expect("unable to create a memory map for input")
    };
    let phase2_size = ceremony_size(&circuit);
    // Read `num_constraints` Lagrange coefficients from the Phase1 Powers of Tau which were
    // prepared for this step. This will fail if Phase 1 was too small.
    debug!("Expected phase2_size: {}", phase2_size);

    let (full_mpc_parameters, query_parameters, all_mpc_parameters) = MPCParameters::<E>::new_from_buffer_chunked(
        circuit,
        &mut phase1_transcript,
        UseCompression::No,
        CheckForCorrectness::No,
        1 << opt.phase1_powers,
        phase2_size,
        opt.chunk_size,
    )
    .unwrap();
    info!("Finished constructing MPC parameters");

    let mut serialized_mpc_parameters = vec![];
    full_mpc_parameters.write(&mut serialized_mpc_parameters).unwrap();
    info!("Serialized `full_mpc_parameters`");

    let mut serialized_query_parameters = vec![];
    match COMPRESSION {
        UseCompression::No => query_parameters.serialize(&mut serialized_query_parameters),
        UseCompression::Yes => query_parameters.serialize(&mut serialized_query_parameters),
    }
    .unwrap();
    info!("Serialized `query_parameters`");

    let contribution_hash = {
        std::fs::File::create(format!("{}.full", opt.challenge_fname))
            .expect("unable to open new challenge hash file")
            .write_all(&serialized_mpc_parameters)
            .expect("unable to write serialized mpc parameters");
        // Get the hash of the contribution, so the user can compare later
        calculate_hash(&serialized_mpc_parameters)
    };
    info!("Hashed `full_mpc_parameters`");

    std::fs::File::create(format!("{}.query", opt.challenge_fname))
        .expect("unable to open new challenge hash file")
        .write_all(&serialized_query_parameters)
        .expect("unable to write serialized mpc parameters");
    info!("Wrote `query_parameters` to file {}", opt.challenge_fname);

    let mut challenge_list_file = std::fs::File::create("phase1").expect("unable to open new challenge list file");

    for (i, chunk) in all_mpc_parameters.iter().enumerate() {
        let mut serialized_chunk = vec![];
        chunk.write(&mut serialized_chunk).expect("unable to write chunk");
        std::fs::File::create(format!("{}.{}", opt.challenge_fname, i))
            .expect("unable to open new challenge hash file")
            .write_all(&serialized_chunk)
            .expect("unable to write serialized mpc parameters");
        info!(
            "Output `mpc_parameter` chunk {} to file {}.{}",
            i, opt.challenge_fname, i
        );
        challenge_list_file
            .write(format!("{}.{}\n", opt.challenge_fname, i).as_bytes())
            .expect("unable to write challenge list");
    }

    std::fs::File::create(format!("{}.{}\n", opt.challenge_hash_fname, "query"))
        .expect("unable to open new challenge hash file")
        .write_all(&contribution_hash)
        .expect("unable to write new challenge hash");

    println!("Wrote a fresh accumulator to challenge file");

    Ok(())
}
