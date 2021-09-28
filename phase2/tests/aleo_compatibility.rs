use phase1::{helpers::testing::setup_verify, Phase1, Phase1Parameters, ProvingSystem};
use phase2::{helpers::testing::TestCircuit, parameters::MPCParameters};
use setup_utils::{derive_rng_from_seed, CheckForCorrectness, Groth16Params, UseCompression};

use snarkvm_algorithms::snark::groth16::{create_random_proof, prepare_verifying_key, verify_proof, ProvingKey};
use snarkvm_curves::{bls12_377::Bls12_377 as AleoBls12_377, bw6_761::BW6_761 as AleoBW6, PairingEngine};

use phase2::{chunked_groth16::verify, parameters::circuit_to_qap};
use rand::{thread_rng, CryptoRng, Rng};
use snarkvm_r1cs::{ConstraintCounter, ConstraintSynthesizer};

fn generate_mpc_parameters<E: PairingEngine, C, R: Rng + CryptoRng>(
    proving_system: ProvingSystem,
    c: C,
    rng: &mut R,
) -> MPCParameters<E>
where
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    let powers = 6; // Powers of tau
    let batch = 4;
    let params = Phase1Parameters::<E>::new_full(proving_system, powers, batch);
    let compressed = UseCompression::Yes;
    // Make 1 power of tau contribution (assume powers of tau gets calculated properly).
    let (_, output, _, _) = setup_verify(compressed, CheckForCorrectness::Full, compressed, &params);
    let accumulator = Phase1::deserialize(&output, compressed, CheckForCorrectness::Full, &params).unwrap();

    // Prepare only the first 32 powers (for whatever reason).
    let groth_params = Groth16Params::<E>::new(
        32,
        accumulator.tau_powers_g1,
        accumulator.tau_powers_g2,
        accumulator.alpha_tau_powers_g1,
        accumulator.beta_tau_powers_g1,
        accumulator.beta_g2,
    )
    .unwrap();
    // Write the transcript to a file.
    let mut writer = vec![];
    groth_params.write(&mut writer, compressed).unwrap();

    // perform the MPC on only the amount of constraints required for the circuit
    let mut counter = ConstraintCounter::default();
    c.clone().generate_constraints(&mut counter).unwrap();
    let phase2_size = std::cmp::max(
        counter.num_constraints,
        counter.num_private_variables + counter.num_public_variables + 1,
    );

    let mut mpc = MPCParameters::<E>::new_from_buffer::<_>(
        c,
        writer.as_mut(),
        compressed,
        CheckForCorrectness::Full,
        32,
        phase2_size,
    )
    .unwrap();

    let before = mpc.clone();
    // It is _not_ safe to use it yet, there must be 1 contribution.
    mpc.contribute(rng).unwrap();

    before.verify(&mpc).unwrap();

    mpc
}

fn generate_mpc_parameters_chunked<E, C>(c: C) -> MPCParameters<E>
where
    E: PairingEngine,
    C: Clone + ConstraintSynthesizer<E::Fr>,
{
    // perform the MPC on only the amount of constraints required for the circuit
    let mut counter = ConstraintCounter::default();
    c.clone().generate_constraints(&mut counter).unwrap();
    let phase2_size = std::cmp::max(
        counter.num_constraints,
        counter.num_private_variables + counter.num_public_variables + 1,
    );

    let powers = 6;
    let batch = 4;
    let params = Phase1Parameters::<E>::new_full(ProvingSystem::Groth16, powers, batch);
    let compressed = UseCompression::Yes;

    // make 1 power of tau contribution (assume powers of tau gets calculated properly)
    let (_, output, _, _) = setup_verify(compressed, CheckForCorrectness::Full, compressed, &params);
    let accumulator = Phase1::deserialize(&output, compressed, CheckForCorrectness::Full, &params).unwrap();

    // prepare only the first 32 powers (for whatever reason)
    let groth_params = Groth16Params::<E>::new(
        1 << powers,
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

    let _ = circuit_to_qap::<E, C>(c.clone()).unwrap();

    let chunk_size = phase2_size / 3;

    let (full_mpc_before, queries, mut mpcs) = MPCParameters::<E>::new_from_buffer_chunked(
        c,
        writer.as_mut(),
        compressed,
        CheckForCorrectness::Full,
        1 << powers,
        phase2_size,
        chunk_size,
    )
    .unwrap();

    let mut full_mpc_before_serialized = vec![];
    full_mpc_before.write(&mut full_mpc_before_serialized).unwrap();

    for mpc in mpcs.iter_mut() {
        let mut rng = derive_rng_from_seed(&[0u8; 32]);
        let before = mpc.clone();
        // it is _not_ safe to use it yet, there must be 1 contribution
        mpc.contribute(&mut rng).unwrap();

        before.verify(&mpc).unwrap();
    }

    let full_mpc_after = MPCParameters::<E>::combine(&queries, &mpcs).unwrap();
    let mut full_mpc_after_serialized = vec![];
    full_mpc_after.write(&mut full_mpc_after_serialized).unwrap();
    verify::<E>(&mut full_mpc_before_serialized, &mut full_mpc_after_serialized, 3).unwrap();

    full_mpc_after
}

fn test_groth16_curve<E: PairingEngine>() {
    for chunked_mode in &[false, true] {
        let rng = &mut thread_rng();
        // Generate the parameters.
        let params: ProvingKey<E> = {
            let c = TestCircuit::<E>(None);
            let setup = match chunked_mode {
                false => generate_mpc_parameters::<E, _, _>(ProvingSystem::Groth16, c, rng),
                true => generate_mpc_parameters_chunked::<E, _>(c),
            };
            setup.get_params().clone()
        };

        // Prepare the verification key (for proof verification)
        let vk = params.vk.clone();
        let pvk = prepare_verifying_key(vk);

        let out = E::Fr::from(25u8);
        let input = E::Fr::from(5u8);

        // Create a proof with these parameters.
        let proof = {
            let c = TestCircuit::<E>(Some(input));
            create_random_proof(&c, &params, rng).unwrap()
        };

        let res = verify_proof(&pvk, &proof, &[out]);
        assert!(res.is_ok());
    }
}

#[test]
fn test_groth16_bls12_377() {
    test_groth16_curve::<AleoBls12_377>()
}

#[test]
fn test_groth16_bw6() {
    test_groth16_curve::<AleoBW6>()
}
