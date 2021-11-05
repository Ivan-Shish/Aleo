use phase2::parameters::MPCParameters;
use setup_utils::{log_2, CheckForCorrectness, Groth16Params, UseCompression};
use snarkvm_algorithms::{SNARK, SRS};
use snarkvm_curves::PairingEngine;
use snarkvm_dpc::{
    parameters::testnet2::{Testnet2DPC, Testnet2Parameters},
    prelude::*,
};
use snarkvm_fields::Field;
use snarkvm_r1cs::{ConstraintCounter, ConstraintSynthesizer};

use gumdrop::Options;
use memmap::MmapOptions;
use phase2::parameters::circuit_to_qap;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;

type AleoInner = <Testnet2Parameters as Parameters>::InnerCurve;
type AleoOuter = <Testnet2Parameters as Parameters>::OuterCurve;

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
    #[options(help = "the path to the phase1 parameters", default = "phase1")]
    pub phase1: String,
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

    #[options(help = "setup the inner or the outer circuit?")]
    pub is_inner: bool,
}

pub fn new(opt: &NewOpts) -> anyhow::Result<()> {
    if opt.is_inner {
        let circuit = InnerCircuit::<Testnet2Parameters>::blank();
        generate_params::<AleoInner, _>(opt, circuit)
    } else {
        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
        let rng = &mut ChaChaRng::from_seed(seed);
        let dpc = Testnet2DPC::load(false)?;

        let noop_circuit = dpc
            .noop_program
            .find_circuit_by_index(0)
            .ok_or(DPCError::MissingNoopCircuit)?;
        let private_program_input = dpc.noop_program.execute_blank(noop_circuit.circuit_id())?;

        let inner_snark_parameters = <Testnet2Parameters as Parameters>::InnerSNARK::setup(
            &InnerCircuit::<Testnet2Parameters>::blank(),
            &mut SRS::CircuitSpecific(rng),
        )?;

        let inner_snark_vk: <<Testnet2Parameters as Parameters>::InnerSNARK as SNARK>::VerifyingKey =
            inner_snark_parameters.1.clone().into();
        let inner_snark_proof = <Testnet2Parameters as Parameters>::InnerSNARK::prove(
            &inner_snark_parameters.0,
            &InnerCircuit::<Testnet2Parameters>::blank(),
            rng,
        )?;

        let circuit =
            OuterCircuit::<Testnet2Parameters>::blank(inner_snark_vk, inner_snark_proof, private_program_input);
        generate_params::<AleoOuter, _>(opt, circuit)
    }
}

pub fn new_challenge(opt: &NewOpts) -> anyhow::Result<()> {
    if opt.is_inner {
        let circuit = InnerCircuit::<Testnet2Parameters>::blank();
        generate_params_chunked::<AleoInner, _>(opt, circuit)
    } else {
        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
        let rng = &mut ChaChaRng::from_seed(seed);
        let dpc = Testnet2DPC::load(false)?;

        let noop_circuit = dpc
            .noop_program
            .find_circuit_by_index(0)
            .ok_or(DPCError::MissingNoopCircuit)?;
        let private_program_input = dpc.noop_program.execute_blank(noop_circuit.circuit_id())?;

        let inner_snark_parameters = <Testnet2Parameters as Parameters>::InnerSNARK::setup(
            &InnerCircuit::<Testnet2Parameters>::blank(),
            &mut SRS::CircuitSpecific(rng),
        )?;

        let inner_snark_vk: <<Testnet2Parameters as Parameters>::InnerSNARK as SNARK>::VerifyingKey =
            inner_snark_parameters.1.clone().into();
        let inner_snark_proof = <Testnet2Parameters as Parameters>::InnerSNARK::prove(
            &inner_snark_parameters.0,
            &InnerCircuit::<Testnet2Parameters>::blank(),
            rng,
        )?;

        let circuit =
            OuterCircuit::<Testnet2Parameters>::blank(inner_snark_vk, inner_snark_proof, private_program_input);
        generate_params_chunked::<AleoOuter, _>(opt, circuit)
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
    let power = log_2(phase2_size) as u32;

    // get the nearest power of 2
    if phase2_size < 2usize.pow(power) {
        2usize.pow(power + 1)
    } else {
        phase2_size
    }
}

pub fn generate_params<E, C>(opt: &NewOpts, circuit: C) -> anyhow::Result<()>
where
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    let phase1_transcript = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&opt.phase1)
        .expect("could not read phase 1 transcript file");
    let mut phase1_transcript = unsafe {
        MmapOptions::new()
            .map_mut(&phase1_transcript)
            .expect("unable to create a memory map for input")
    };
    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create_new(true)
        .open(&opt.output)
        .expect("could not open file for writing the MPC parameters ");

    let phase2_size = ceremony_size(&circuit);
    let keypair = circuit_to_qap::<E, _>(circuit)?;
    // Read `num_constraints` Lagrange coefficients from the Phase1 Powers of Tau which were
    // prepared for this step. This will fail if Phase 1 was too small.
    let phase1 = Groth16Params::<E>::read(
        &mut phase1_transcript,
        COMPRESSION,
        CheckForCorrectness::No, // No need to check for correctness, since this has been processed by the coordinator.
        2usize.pow(opt.phase1_size),
        phase2_size,
    )?;

    // Generate the initial transcript
    let mpc = MPCParameters::new(keypair, phase1)?;
    mpc.write(&mut output)?;

    Ok(())
}

pub fn generate_params_chunked<E, C>(opt: &NewOpts, circuit: C) -> anyhow::Result<()>
where
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    let phase1_transcript = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&opt.phase1)
        .expect("could not read phase 1 transcript file");
    let mut phase1_transcript = unsafe {
        MmapOptions::new()
            .map_mut(&phase1_transcript)
            .expect("unable to create a memory map for input")
    };
    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create_new(true)
        .open(&opt.output)
        .expect("could not open file for writing the MPC parameters ");

    let phase2_size = ceremony_size(&circuit);
    // Read `num_constraints` Lagrange coefficients from the Phase1 Powers of Tau which were
    // prepared for this step. This will fail if Phase 1 was too small.
    let phase1 = Groth16Params::<E>::read(
        &mut phase1_transcript,
        COMPRESSION,
        CheckForCorrectness::No, // No need to check for correctness, since this has been processed by the coordinator.
        2usize.pow(opt.phase1_size),
        phase2_size,
    )?;

    let compressed = UseCompression::Yes;
    let mut writer = vec![];
    phase1.write(&mut writer, compressed).unwrap();
    let chunk_size = phase2_size / 3;

    let (full_mpc_parameters, _, _) = MPCParameters::<E>::new_from_buffer_chunked(
        circuit,
        writer.as_mut(),
        UseCompression::No,
        CheckForCorrectness::No,
        2usize.pow(opt.phase1_size),
        phase2_size,
        chunk_size,
    )
    .unwrap();

    // Generate the initial transcript
    //let mpc = MPCParameters::new(keypair, phase1)?;
    full_mpc_parameters.write(&mut output)?;

    Ok(())
}
