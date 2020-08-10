use crate::{Phase1, Phase1Parameters, PublicKey};
use snark_utils::{UseCompression, *};

use zexe_algebra::PairingEngine;
#[cfg(test)]
use zexe_algebra::{AffineCurve, ProjectiveCurve};
#[cfg(test)]
use zexe_algebra_core::UniformRand;

use rand::thread_rng;
#[cfg(test)]
use rand::Rng;

/// Returns a random affine curve point from the provided RNG.
#[cfg(test)]
pub fn random_point<C: AffineCurve>(rng: &mut impl Rng) -> C {
    C::Projective::rand(rng).into_affine()
}

/// Returns a random affine curve point vector from the provided RNG.
#[cfg(test)]
pub fn random_point_vec<C: AffineCurve>(size: usize, rng: &mut impl Rng) -> Vec<C> {
    (0..size).map(|_| random_point(rng)).collect()
}

/// Helper for testing verification of a transformation
/// it creates an initial accumulator and contributes to it
/// the test must call verify on the returned values.
pub fn setup_verify<E: PairingEngine>(
    compressed_input: UseCompression,
    compressed_output: UseCompression,
    parameters: &Phase1Parameters<E>,
) -> (Vec<u8>, Vec<u8>, PublicKey<E>, GenericArray<u8, U64>) {
    let (input, _) = generate_input(&parameters, compressed_input);
    let mut output = generate_output(&parameters, compressed_output);

    // Construct our keypair
    let current_accumulator_hash = blank_hash();
    let mut rng = thread_rng();
    let (pubkey, privkey) =
        Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

    // transform the accumulator
    Phase1::computation(
        &input,
        &mut output,
        compressed_input,
        compressed_output,
        &privkey,
        parameters,
    )
    .unwrap();
    // ensure that the key is not available to the verifier
    drop(privkey);

    (input, output, pubkey, current_accumulator_hash)
}

/// Helper to initialize an accumulator and return both the struct and its serialized form.
pub fn generate_input<E: PairingEngine>(
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

/// Helper to initialize an empty output accumulator, to be used for contributions.
pub fn generate_output<E: PairingEngine>(parameters: &Phase1Parameters<E>, compressed: UseCompression) -> Vec<u8> {
    let expected_response_length = parameters.get_length(compressed);
    vec![0; expected_response_length]
}

/// Helper to generate a random accumulator for Phase 1 given its parameters.
#[cfg(test)]
pub fn generate_random_accumulator<E: PairingEngine>(
    parameters: &Phase1Parameters<E>,
    compressed: UseCompression,
) -> (Vec<u8>, Phase1<E>) {
    let tau_g1_size = parameters.powers_g1_length;
    let other_size = parameters.powers_length;
    let rng = &mut thread_rng();
    let acc = Phase1 {
        tau_powers_g1: random_point_vec(tau_g1_size, rng),
        tau_powers_g2: random_point_vec(other_size, rng),
        alpha_tau_powers_g1: random_point_vec(other_size, rng),
        beta_tau_powers_g1: random_point_vec(other_size, rng),
        beta_g2: random_point(rng),
        hash: blank_hash(),
        parameters,
    };
    let len = parameters.get_length(compressed);
    let mut buf = vec![0; len];
    acc.serialize(&mut buf, compressed, parameters).unwrap();
    (buf, acc)
}
