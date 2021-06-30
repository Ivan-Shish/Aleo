use std::fmt::Debug;

use anyhow::{anyhow, Result};
use futures::{Sink, SinkExt};
use phase1::{Phase1, Phase1Parameters, ProvingSystem};
use setup1_shared::reliability::{ContributorMessage, ContributorMessageName};
use setup_utils::{blank_hash, derive_rng_from_seed, CheckForCorrectness, UseCompression};
use tokio_tungstenite::tungstenite::protocol::Message;
use zexe_algebra::{Bls12_377, PairingEngine};

const TOTAL_SIZE: usize = 14; // Proof of Work takes roughly 20 seconds if TOTAL_SIZE=14
const BATCH_SIZE: usize = 2;

fn calculate_powers_of_tau<E: PairingEngine>(data: Vec<u8>) -> Vec<u8> {
    let parameters = Phase1Parameters::<E>::new_full(ProvingSystem::Marlin, TOTAL_SIZE, BATCH_SIZE);
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
    let mut rng = derive_rng_from_seed(&data);
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

pub(super) async fn check<W>(write_half: &mut W, data: Vec<u8>) -> Result<()>
where
    W: Sink<Message> + Unpin,
    W::Error: Debug,
{
    let calculated = calculate_powers_of_tau::<Bls12_377>(data);
    let challenge = ContributorMessage {
        name: ContributorMessageName::CpuChallenge,
        data: calculated,
    };
    let response = Message::binary(challenge.to_vec());
    write_half.send(response).await.map_err(|e| anyhow!("{:?}", e))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::reliability::cpu::calculate_powers_of_tau;
    use std::time::Instant;
    use zexe_algebra::Bls12_377;

    const DATA_LENGTH: usize = 64;
    const OUTPUT_LENGTH: usize = 790192;

    #[test]
    fn test_calculate_powers_of_tau() {
        let data: Vec<u8> = vec![0; DATA_LENGTH];
        let now = Instant::now();
        let output = calculate_powers_of_tau::<Bls12_377>(data);
        let elapsed = now.elapsed();
        println!("Proof of Work took: {:.2?}", elapsed);
        assert_eq!(output.len(), OUTPUT_LENGTH);
        let expected = [12, 53, 232, 21, 121, 10, 125, 129];
        assert_eq!(output[OUTPUT_LENGTH - expected.len()..], expected);
    }
}
