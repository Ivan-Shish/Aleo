use crate::{curve_from_str, proving_system_from_str, CurveKind};
use phase1::{Phase1, Phase1Parameters, ProvingSystem};
use setup_utils::{calculate_hash, get_rng, user_system_randomness, CheckForCorrectness, UseCompression};

use zexe_algebra::{Bls12_377, PairingEngine, BW6_761};

use rand::Rng;
use wasm_bindgen::prelude::*;

const COMPRESSED_INPUT: UseCompression = UseCompression::No;
const COMPRESSED_OUTPUT: UseCompression = UseCompression::Yes;
const CHECK_INPUT_CORRECTNESS: CheckForCorrectness = CheckForCorrectness::No;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[derive(Serialize)]
pub struct ContributionResponse {
    current_accumulator_hash: Vec<u8>,
    response: Vec<u8>,
    contribution_hash: Vec<u8>,
}

#[wasm_bindgen]
pub struct Phase1WASM {}

#[wasm_bindgen]
impl Phase1WASM {
    #[wasm_bindgen]
    pub fn contribute(
        curve_kind: &str,
        proving_system: &str,
        batch_size: usize,
        power: usize,
        challenge: &[u8],
    ) -> Result<JsValue, JsValue> {
        let rng = get_rng(&user_system_randomness());
        let proving_system = proving_system_from_str(proving_system).expect("invalid proving system");
        let res = match curve_from_str(curve_kind).expect("invalid curve_kind") {
            CurveKind::Bls12_377 => contribute_challenge(
                &challenge,
                &get_parameters::<Bls12_377>(proving_system, batch_size, power),
                rng,
            ),
            CurveKind::BW6 => contribute_challenge(
                &challenge,
                &get_parameters::<BW6_761>(proving_system, batch_size, power),
                rng,
            ),
        };
        return Ok(JsValue::from_serde(&res.ok().unwrap()).unwrap());
    }
}

pub fn get_parameters<E: PairingEngine>(
    proving_system: ProvingSystem,
    power: usize,
    batch_size: usize,
) -> Phase1Parameters<E> {
    Phase1Parameters::<E>::new_full(proving_system, power, batch_size)
}

pub fn contribute_challenge<E: PairingEngine + Sync>(
    challenge: &[u8],
    parameters: &Phase1Parameters<E>,
    mut rng: impl Rng,
) -> Result<ContributionResponse, String> {
    let expected_challenge_length = match COMPRESSED_INPUT {
        UseCompression::Yes => parameters.contribution_size,
        UseCompression::No => parameters.accumulator_size,
    };

    if challenge.len() != expected_challenge_length {
        return Err(format!(
            "The size of challenge file should be {}, but it's {}, so something isn't right.",
            expected_challenge_length,
            challenge.len()
        ));
    }

    let required_output_length = match COMPRESSED_OUTPUT {
        UseCompression::Yes => parameters.contribution_size,
        UseCompression::No => parameters.accumulator_size + parameters.public_key_size,
    };

    let mut response: Vec<u8> = vec![];
    let current_accumulator_hash = calculate_hash(&challenge);

    for i in 0..required_output_length {
        response.push(current_accumulator_hash[i % current_accumulator_hash.len()]);
    }

    // Construct our keypair using the RNG we created above
    let (public_key, private_key): (phase1::PublicKey<E>, phase1::PrivateKey<E>) =
        match Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()) {
            Ok(pair) => pair,
            Err(_) => return Err("could not generate keypair".to_string()),
        };

    // This computes a transformation and writes it
    match Phase1::computation(
        &challenge,
        &mut response,
        COMPRESSED_INPUT,
        COMPRESSED_OUTPUT,
        CHECK_INPUT_CORRECTNESS,
        &private_key,
        &parameters,
    ) {
        Ok(_) => match public_key.write(&mut response, COMPRESSED_OUTPUT, &parameters) {
            Ok(_) => {
                let contribution_hash = calculate_hash(&response);

                return Ok(ContributionResponse {
                    current_accumulator_hash: current_accumulator_hash.as_slice().iter().cloned().collect(),
                    response,
                    contribution_hash: contribution_hash.as_slice().iter().cloned().collect(),
                });
            }
            Err(_) => {
                return Err("unable to write public key".to_string());
            }
        },
        Err(_) => {
            return Err("must contribute with the key".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;
    use setup_utils::{batch_exp, blank_hash, generate_powers_of_tau};
    use zexe_algebra::{batch_inversion, AffineCurve, Field, ProjectiveCurve};

    fn generate_input<E: PairingEngine>(
        parameters: &Phase1Parameters<E>,
        compressed: UseCompression,
    ) -> (Vec<u8>, Phase1<E>) {
        let len = parameters.get_length(compressed);
        let mut output = vec![0; len];
        Phase1::initialization(&mut output, compressed, &parameters).unwrap();
        let mut input = vec![0; len];
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

        let deserialized =
            Phase1::deserialize(&output, COMPRESSED_OUTPUT, CHECK_INPUT_CORRECTNESS, &parameters).unwrap();

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
                let mut g2_inverse_powers = (0..parameters.total_size_in_log2)
                    .map(|i| privkey.tau.pow([parameters.powers_length as u64 - 1 - (1 << i)]))
                    .collect::<Vec<_>>();
                batch_inversion(&mut g2_inverse_powers);
                batch_exp(&mut before.tau_powers_g2[..2], &tau_powers[0..2], None).unwrap();
                batch_exp(
                    &mut before.tau_powers_g2[2..],
                    &g2_inverse_powers[0..parameters.total_size_in_log2],
                    None,
                )
                .unwrap();
                batch_exp(&mut before.alpha_tau_powers_g1, &tau_powers[0..3], Some(&privkey.alpha)).unwrap();
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
}
