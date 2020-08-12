use crate::{curve_from_str, CurveKind};
use phase1::{Phase1, Phase1Parameters};
use setup_utils::{calculate_hash, get_rng, user_system_randomness, UseCompression};

#[cfg(test)]
use mocktopus::macros::*;

use zexe_algebra::{Bls12_377, PairingEngine, BW6_761};

use rand::Rng;
use wasm_bindgen::prelude::*;

const COMPRESSED_INPUT: UseCompression = UseCompression::No;
const COMPRESSED_OUTPUT: UseCompression = UseCompression::Yes;

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
    pub fn contribute(curve_kind: &str, batch_size: usize, power: usize, challenge: &[u8]) -> Result<JsValue, JsValue> {
        let rng = get_rng(&user_system_randomness());
        let res = match curve_from_str(curve_kind).expect("invalid curve_kind") {
            CurveKind::Bls12_377 => {
                contribute_challenge(&challenge, &get_parameters::<Bls12_377>(batch_size, power), rng)
            }
            CurveKind::BW6 => contribute_challenge(&challenge, &get_parameters::<BW6_761>(batch_size, power), rng),
        };
        return Ok(JsValue::from_serde(&res.ok().unwrap()).unwrap());
    }
}

pub fn get_parameters<E: PairingEngine>(batch_size: usize, power: usize) -> Phase1Parameters<E> {
    Phase1Parameters::<E>::new(power, batch_size)
}

#[cfg_attr(test, mockable)]
pub fn key_generation<R: Rng, E: PairingEngine + Sync>(
    rng: &mut R,
    current_accumulator_hash: &[u8],
) -> Result<(phase1::PublicKey<E>, phase1::PrivateKey<E>), String> {
    match Phase1::key_generation(rng, current_accumulator_hash.as_ref()) {
        Ok(pair) => Ok(pair),
        Err(_) => return Err("could not generate keypair".to_string()),
    }
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
        match key_generation(&mut rng, current_accumulator_hash.as_ref()) {
            Ok(pair) => pair,
            Err(_) => return Err("could not generate keypair".to_string()),
        };

    // this computes a transformation and writes it
    match Phase1::computation(
        &challenge,
        &mut response,
        COMPRESSED_INPUT,
        COMPRESSED_OUTPUT,
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

    use mocktopus::mocking::*;
    use rand::{rngs::ThreadRng, thread_rng};
    use setup_utils::{batch_exp, blank_hash, generate_powers_of_tau};
    use zexe_algebra::{AffineCurve, ProjectiveCurve};

    fn generate_input<E: PairingEngine>(
        parameters: &Phase1Parameters<E>,
        compressed: UseCompression,
    ) -> (Vec<u8>, Phase1<E>) {
        let len = parameters.get_length(compressed);
        let mut output = vec![0; len];
        Phase1::initialization(&mut output, compressed, &parameters).unwrap();
        let mut input = vec![0; len];
        input.copy_from_slice(&output);
        let before = Phase1::deserialize(&output, compressed, &parameters).unwrap();
        (input, before)
    }

    fn contribute_challenge_test<E: PairingEngine + Sync>(parameters: &Phase1Parameters<E>) {
        // Get a non-mutable copy of the initial accumulator state.
        let (input, mut before) = generate_input(&parameters, COMPRESSED_INPUT);

        let mut rng = thread_rng();
        // Construct our keypair using the RNG we created above
        let current_accumulator_hash = blank_hash();

        let (public_key, private_key): (phase1::PublicKey<E>, phase1::PrivateKey<E>) =
            Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

        let privkey = private_key.clone();
        key_generation::<ThreadRng, E>
            .mock_safe(move |_, _| MockResult::Return(Ok((public_key.clone(), private_key.clone()))));

        let output = contribute_challenge(&input, parameters, rng).unwrap().response;

        let deserialized = Phase1::deserialize(&output, COMPRESSED_OUTPUT, &parameters).unwrap();

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

        assert_eq!(deserialized, before);
    }

    #[wasm_bindgen_test]
    pub fn test_phase1_contribute_bls12_377() {
        contribute_challenge_test(&get_parameters::<Bls12_377>(2, 2));
        // Works even when the batch is larger than the powers
        contribute_challenge_test(&get_parameters::<Bls12_377>(6, 128));
    }

    #[wasm_bindgen_test]
    fn test_phase1_contribute_bw6_761() {
        contribute_challenge_test(&get_parameters::<BW6_761>(2, 2));
        // Works even when the batch is larger than the powers
        contribute_challenge_test(&get_parameters::<Bls12_377>(6, 128));
    }
}
