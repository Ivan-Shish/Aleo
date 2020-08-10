use phase2::parameters::MPCParameters;
use powersoftau::{Phase1, Phase1Parameters};
use snark_utils::{Groth16Params, UseCompression};
use test_helpers::{setup_verify, TestCircuit};

use rand::{thread_rng, Rng};
use zexe_algebra::{serialize::CanonicalSerialize, Bls12_377, PairingEngine, BW6_761};
use zexe_groth16::Parameters;

// SnarkOS data types
use snarkos_algorithms::snark::groth16::{
    create_random_proof,
    prepare_verifying_key,
    verify_proof,
    Parameters as AleoGroth16Params,
};
use snarkos_curves::{bls12_377::Bls12_377 as AleoBls12_377, bw6_761::BW6_761 as AleoBW6};
use snarkos_models::{
    curves::PairingEngine as AleoPairingEngine,
    gadgets::r1cs::{ConstraintCounter, ConstraintSynthesizer},
};
use snarkos_utilities::serialize::CanonicalDeserialize;

fn generate_mpc_parameters<Aleo, E, C, R: Rng>(c: C, rng: &mut R) -> MPCParameters<E>
where
    Aleo: AleoPairingEngine,
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<Aleo::Fr>,
{
    let powers = 6; // powers of tau
    let batch = 4;
    let params = Phase1Parameters::<E>::new(powers, batch);
    let compressed = UseCompression::Yes;
    // make 1 power of tau contribution (assume powers of tau gets calculated properly)
    let (_, output, _, _) = setup_verify(compressed, compressed, &params);
    let accumulator = Phase1::deserialize(&output, compressed, &params).unwrap();

    // prepare only the first 32 powers (for whatever reason)
    let groth_params = Groth16Params::<E>::new(
        32,
        accumulator.tau_powers_g1,
        accumulator.tau_powers_g2,
        accumulator.alpha_tau_powers_g1,
        accumulator.beta_tau_powers_g1,
        accumulator.beta_g2,
    )
    .unwrap();
    // write the transcript to a file
    let mut writer = vec![];
    groth_params.write(&mut writer, compressed).unwrap();

    // perform the MPC on only the amount of constraints required for the circuit
    let mut counter = ConstraintCounter::new();
    c.clone().generate_constraints(&mut counter).unwrap();
    let phase2_size = std::cmp::max(counter.num_constraints, counter.num_aux + counter.num_inputs + 1);

    let mut mpc =
        MPCParameters::<E>::new_from_buffer::<Aleo, _>(c, writer.as_mut(), compressed, 32, phase2_size).unwrap();

    let before = mpc.clone();
    // it is _not_ safe to use it yet, there must be 1 contribution
    mpc.contribute(rng).unwrap();

    before.verify(&mpc).unwrap();

    mpc
}

#[test]
fn test_groth_bls12_377() {
    groth_test_curve::<AleoBls12_377, Bls12_377>()
}

#[test]
fn test_groth_bw6() {
    groth_test_curve::<AleoBW6, BW6_761>()
}

fn groth_test_curve<Aleo: AleoPairingEngine, E: PairingEngine>() {
    let rng = &mut thread_rng();
    // generate the params
    let params: Parameters<E> = {
        let c = TestCircuit::<Aleo>(None);
        let setup = generate_mpc_parameters::<Aleo, E, _, _>(c, rng);
        setup.get_params().clone()
    };

    // convert the Zexe params to snarkOS params
    let mut v = Vec::new();
    params.serialize(&mut v).unwrap();
    let params = AleoGroth16Params::<Aleo>::deserialize(&mut &v[..]).unwrap();

    // Prepare the verification key (for proof verification)
    let pvk = prepare_verifying_key(&params.vk);

    // Create a proof with these params
    let proof = {
        let c = TestCircuit::<Aleo>(Some(Aleo::Fr::from(5)));
        create_random_proof(c, &params, rng).unwrap()
    };

    let res = verify_proof(&pvk, &proof, &[Aleo::Fr::from(25u8)]);
    assert!(res.is_ok());
}
