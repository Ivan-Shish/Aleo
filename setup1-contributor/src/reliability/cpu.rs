use std::{
    fmt::Debug,
    io::{BufReader, Read},
};

use anyhow::{anyhow, Result};
use futures::{Sink, SinkExt};
use tokio_tungstenite::tungstenite::protocol::Message;

use setup1_shared::{
    proof_of_work::calculate_powers_of_tau,
    reliability::{ContributorMessage, ContributorMessageName},
};
use snarkvm_curves::bls12_377::Bls12_377;

/// Performs the powers of tau computation and writes the result
/// to the socket
pub(super) async fn check<W>(write_half: &mut W, data: Vec<u8>) -> Result<()>
where
    W: Sink<Message> + Unpin,
    W::Error: Debug,
{
    println!("Computing challenge...");
    let mut reader = BufReader::new(&data[..8]);
    let mut total_size_bytes = [0; 4];
    reader.read_exact(&mut total_size_bytes)?;
    let total_size = u32::from_be_bytes(total_size_bytes) as usize;
    let mut batch_size_bytes = [0; 4];
    reader.read_exact(&mut batch_size_bytes)?;
    let batch_size = u32::from_be_bytes(batch_size_bytes) as usize;
    // Proof of Work takes roughly 20 seconds if TOTAL_SIZE=14
    let calculated = calculate_powers_of_tau::<Bls12_377>(&data[8..], total_size, batch_size);
    println!("Done");
    let challenge = ContributorMessage {
        name: ContributorMessageName::CpuChallenge,
        data: calculated,
    };
    let response = Message::binary(challenge.to_vec());
    write_half.send(response).await.map_err(|e| anyhow!("{:?}", e))?;
    Ok(())
}
