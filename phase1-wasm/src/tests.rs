use crate::phase1::*;
use phase1::{Phase1, Phase1Parameters, ProvingSystem};
use setup_utils::{batch_exp, blank_hash, generate_powers_of_tau, UseCompression};

use zexe_algebra::{batch_inversion, AffineCurve, Bls12_377, Field, PairingEngine, ProjectiveCurve, BW6_761};

use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use wasm_bindgen_test::*;

fn generate_input<E: PairingEngine>(
    parameters: &Phase1Parameters<E>,
    compressed: UseCompression,
) -> (Vec<u8>, Phase1<E>) {
    let length = parameters.get_length(compressed);

    let mut output = vec![0; length];
    Phase1::initialization(&mut output, compressed, &parameters).unwrap();

    let mut input = vec![0; length];
    input.copy_from_slice(&output);

    let before = Phase1::deserialize(&output, compressed, CHECK_INPUT_CORRECTNESS, &parameters).unwrap();
    (input, before)
}

fn contribute_challenge_test<E: PairingEngine + Sync>(parameters: &Phase1Parameters<E>) {
    // Get a non-mutable copy of the initial accumulator state.
    let (input, mut before) = generate_input(&parameters, COMPRESSED_INPUT);

    let mut rng = XorShiftRng::seed_from_u64(0);
    // Construct our keypair using the RNG we created above
    let current_accumulator_hash = blank_hash();

    let (_, privkey): (phase1::PublicKey<E>, phase1::PrivateKey<E>) =
        Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

    let output = contribute_challenge(&input, parameters, XorShiftRng::seed_from_u64(0))
        .unwrap()
        .response;

    let deserialized = Phase1::deserialize(&output, COMPRESSED_OUTPUT, CHECK_INPUT_CORRECTNESS, &parameters).unwrap();

    match parameters.proving_system {
        ProvingSystem::Groth16 => {
            let tau_powers = generate_powers_of_tau::<E>(&privkey.tau, 0, parameters.powers_g1_length);
            batch_exp(
                &mut before.tau_powers_g1,
                &tau_powers[0..parameters.powers_g1_length],
                None,
            )
            .unwrap();
            batch_exp(
                &mut before.tau_powers_g2,
                &tau_powers[0..parameters.powers_length],
                None,
            )
            .unwrap();
            batch_exp(
                &mut before.alpha_tau_powers_g1,
                &tau_powers[0..parameters.powers_length],
                Some(&privkey.alpha),
            )
            .unwrap();
            batch_exp(
                &mut before.beta_tau_powers_g1,
                &tau_powers[0..parameters.powers_length],
                Some(&privkey.beta),
            )
            .unwrap();
            before.beta_g2 = before.beta_g2.mul(privkey.beta).into_affine();
        }
        ProvingSystem::Marlin => {
            let tau_powers = generate_powers_of_tau::<E>(&privkey.tau, 0, parameters.powers_length);
            batch_exp(
                &mut before.tau_powers_g1,
                &tau_powers[0..parameters.powers_length],
                None,
            )
            .unwrap();

            let degree_bound_powers = (0..parameters.total_size_in_log2)
                .map(|i| privkey.tau.pow([parameters.powers_length as u64 - 1 - (1 << i) + 2]))
                .collect::<Vec<_>>();
            let mut g2_inverse_powers = degree_bound_powers.clone();
            batch_inversion(&mut g2_inverse_powers);
            batch_exp(&mut before.tau_powers_g2[..2], &tau_powers[0..2], None).unwrap();
            batch_exp(
                &mut before.tau_powers_g2[2..],
                &g2_inverse_powers[0..parameters.total_size_in_log2],
                None,
            )
            .unwrap();

            let g1_degree_powers = degree_bound_powers
                .into_iter()
                .map(|f| vec![f, f * &privkey.tau, f * &privkey.tau * &privkey.tau])
                .flatten()
                .collect::<Vec<_>>();
            batch_exp(
                &mut before.alpha_tau_powers_g1[3..3 + 3 * parameters.total_size_in_log2],
                &g1_degree_powers,
                Some(&privkey.alpha),
            )
            .unwrap();
            batch_exp(
                &mut before.alpha_tau_powers_g1[0..3],
                &tau_powers[0..3],
                Some(&privkey.alpha),
            )
            .unwrap();
        }
    }
    assert_eq!(deserialized, before);
}

#[wasm_bindgen_test]
pub fn test_phase1_contribute_bls12_377() {
    for proving_system in &[ProvingSystem::Groth16, ProvingSystem::Marlin] {
        contribute_challenge_test(&get_parameters::<Bls12_377>(*proving_system, 2, 2));
        // Works even when the batch is larger than the powers
        contribute_challenge_test(&get_parameters::<Bls12_377>(*proving_system, 6, 128));
    }
}

#[wasm_bindgen_test]
fn test_phase1_contribute_bw6_761() {
    for proving_system in &[ProvingSystem::Groth16, ProvingSystem::Marlin] {
        contribute_challenge_test(&get_parameters::<BW6_761>(*proving_system, 2, 2));
        // Works even when the batch is larger than the powers
        contribute_challenge_test(&get_parameters::<BW6_761>(*proving_system, 6, 128));
    }
}
