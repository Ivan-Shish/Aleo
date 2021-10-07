use phase1::{Phase1, Phase1Parameters, ProvingSystem};
use setup_utils::{blank_hash, derive_rng_from_seed, CheckForCorrectness, UseCompression};
use snarkvm_curves::PairingEngine;

pub fn calculate_powers_of_tau<E: PairingEngine>(data: &[u8], total_size: usize, batch_size: usize) -> Vec<u8> {
    let parameters = Phase1Parameters::<E>::new_full(ProvingSystem::Marlin, total_size, batch_size);
    let expected_response_length = parameters.get_length(UseCompression::Yes);

    // Get a non-mutable copy of the initial accumulator state.
    let len = parameters.get_length(UseCompression::Yes);
    let mut output = vec![0; len];
    Phase1::initialization(&mut output, UseCompression::Yes, &parameters).unwrap();
    let mut input = vec![0; len];
    input.copy_from_slice(&output);
    Phase1::deserialize(&output, UseCompression::Yes, CheckForCorrectness::No, &parameters).unwrap();

    let mut output = vec![0; expected_response_length];

    // Construct our keypair using the RNG we created above
    let current_accumulator_hash = blank_hash();
    let mut rng = derive_rng_from_seed(data);
    let (_, privkey) =
        Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");
    Phase1::computation(
        &input,
        &mut output,
        UseCompression::Yes,
        UseCompression::Yes,
        CheckForCorrectness::No,
        &privkey,
        &parameters,
    )
    .unwrap();
    output
}

#[cfg(test)]
mod test {
    use super::calculate_powers_of_tau;
    use snarkvm_curves::bls12_377::Bls12_377;

    use std::time::Instant;

    const TOTAL_SIZE: usize = 14; // Proof of Work takes roughly 20 seconds if TOTAL_SIZE=14
    const BATCH_SIZE: usize = 2;

    const DATA_LENGTH: usize = 64;
    const OUTPUT_LENGTH: usize = 790192;

    #[test]
    fn test_calculate_powers_of_tau() {
        let data: Vec<u8> = vec![0; DATA_LENGTH];
        let now = Instant::now();
        let output = calculate_powers_of_tau::<Bls12_377>(&data, TOTAL_SIZE, BATCH_SIZE);
        let elapsed = now.elapsed();
        println!("Proof of Work took: {:.2?}", elapsed);
        assert_eq!(output.len(), OUTPUT_LENGTH);
        let expected = [53, 123, 154, 189, 96, 121, 231, 0];
        assert_eq!(output[OUTPUT_LENGTH - expected.len()..], expected);
    }
}
