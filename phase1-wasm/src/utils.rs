use crate::structures::*;
use blake2::{digest::generic_array::GenericArray, Blake2b, Blake2s, Digest};
use rand::{CryptoRng, Rng};
use snarkvm_dpc::{testnet2::Testnet2, Address, PrivateKey};
use snarkvm_utilities::ToBytes;
use std::convert::TryFrom;
use wasm_bindgen::prelude::*;

pub fn generate_confirmation_key(address: &Address<Testnet2>, private_key: &PrivateKey<Testnet2>) -> String {
    let concatenated = format!("{}{}", address.to_string(), private_key.to_string());
    let mut hasher = Blake2s::new();
    hasher.update(concatenated.as_bytes());
    let bytes = hasher.finalize().to_vec();
    hex::encode(&bytes)
}

pub fn get_authorization_value<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    method: &str,
    path: &str,
    rng: &mut R,
) -> Result<String, JsValue> {
    let address = Address::try_from(private_key)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
        .to_string();

    let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());
    let signature = hex::encode(
        &private_key
            .sign(message.as_bytes(), rng)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
            .to_bytes_le()
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?,
    );

    let authorization = format!("Aleo {}:{}", address, signature);
    Ok(authorization)
}

pub fn sign_contribution_state<R: Rng + CryptoRng>(
    signing_key: &PrivateKey<Testnet2>,
    challenge_hash: &[u8],
    response_hash: &[u8],
    next_challenge_hash: Option<Vec<u8>>,
    rng: &mut R,
) -> Result<ContributionFileSignature, JsValue> {
    let contribution_state =
        ContributionState::new(challenge_hash.to_vec(), response_hash.to_vec(), next_challenge_hash)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    let message = contribution_state
        .signature_message()
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

    let signature = hex::encode(
        &signing_key
            .sign(message.as_bytes(), rng)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
            .to_bytes_le()
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?,
    );

    let contribution_file_signature = ContributionFileSignature::new(signature, contribution_state)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

    Ok(contribution_file_signature)
}

pub fn calculate_hash(input_map: &[u8]) -> Vec<u8> {
    let chunk_size = 1 << 30; // read by 1GB from map
    let mut hasher = Blake2b::default();
    for chunk in input_map.chunks(chunk_size) {
        hasher.update(&chunk);
    }
    hasher.finalize().to_vec()
}
