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

use memmap::MmapOptions;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use setup_utils::calculate_hash;
use std::{fs::OpenOptions, io::Write};

type AleoInner = <Testnet2 as Network>::InnerCurve;
type AleoOuter = <Testnet2 as Network>::OuterCurve;

const COMPRESSION: UseCompression = UseCompression::No;

pub const SEED_LENGTH: usize = 32;
pub type Seed = [u8; SEED_LENGTH];

pub fn new(
    is_inner: bool,
    challenge_filename: &str,
    challenge_hash_filename: &str,
    challenge_list_filename: &str,
    chunk_size: usize,
    phase1_filename: &str,
    phase1_powers: usize,
    circuit_filename: &str,
) -> anyhow::Result<()> {
    if is_inner {
        let circuit = InnerCircuit::<Testnet2>::blank();
        generate_params_chunked::<AleoInner, _>(
            is_inner,
            challenge_filename,
            challenge_hash_filename,
            challenge_list_filename,
            chunk_size,
            phase1_filename,
            phase1_powers,
            circuit_filename,
            circuit,
        )
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

        generate_params_chunked::<AleoOuter, _>(
            is_inner,
            challenge_filename,
            challenge_hash_filename,
            challenge_list_filename,
            chunk_size,
            phase1_filename,
            phase1_powers,
            circuit_filename,
            outer_circuit,
        )
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

pub fn generate_params_chunked<E, C>(
    is_inner: bool,
    challenge_filename: &str,
    challenge_hash_filename: &str,
    challenge_list_filename: &str,
    chunk_size: usize,
    phase1_filename: &str,
    phase1_powers: usize,
    circuit_filename: &str,
    circuit: C,
) -> anyhow::Result<()>
where
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    let phase1_transcript = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&phase1_filename)
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
        1 << phase1_powers,
        phase2_size,
        chunk_size,
    )
    .unwrap();
    info!("Finished constructing MPC parameters");

    let mut serialized_mpc_parameters = vec![];
    full_mpc_parameters.write(&mut serialized_mpc_parameters).unwrap();
    info!("Serialized `full_mpc_parameters`");

    let mut serialized_query_parameters = vec![];
    match COMPRESSION {
        UseCompression::No => query_parameters.serialize_uncompressed(&mut serialized_query_parameters),
        UseCompression::Yes => query_parameters.serialize(&mut serialized_query_parameters),
    }
    .unwrap();
    info!("Serialized `query_parameters`");

    let contribution_hash = {
        std::fs::File::create(format!("{}.full", challenge_filename))
            .expect("unable to open new challenge hash file")
            .write_all(&serialized_mpc_parameters)
            .expect("unable to write serialized mpc parameters");
        // Get the hash of the contribution, so the user can compare later
        calculate_hash(&serialized_mpc_parameters)
    };
    info!("Hashed `full_mpc_parameters`");

    std::fs::File::create(format!("{}.query", challenge_filename))
        .expect("unable to open new challenge hash file")
        .write_all(&serialized_query_parameters)
        .expect("unable to write serialized mpc parameters");
    info!("Wrote `query_parameters` to file {}.query", challenge_filename);

    let mut challenge_list_file = std::fs::File::create("phase1").expect("unable to open new challenge list file");

    for (i, chunk) in all_mpc_parameters.iter().enumerate() {
        let mut serialized_chunk = vec![];
        chunk.write(&mut serialized_chunk).expect("unable to write chunk");
        std::fs::File::create(format!("{}.{}", challenge_filename, i))
            .expect("unable to open new challenge hash file")
            .write_all(&serialized_chunk)
            .expect("unable to write serialized mpc parameters");
        info!(
            "Output `mpc_parameter` chunk {} to file {}.{}",
            i, challenge_filename, i
        );
        challenge_list_file
            .write(format!("{}.{}\n", challenge_filename, i).as_bytes())
            .expect("unable to write challenge list");
    }

    std::fs::File::create(format!("{}.{}\n", challenge_hash_filename, "query"))
        .expect("unable to open new challenge hash file")
        .write_all(&contribution_hash)
        .expect("unable to write new challenge hash");

    println!("Wrote a fresh accumulator to challenge file");

    Ok(())
}
