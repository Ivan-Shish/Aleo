use crate::structures::*;
use blake2::{Blake2b, Blake2s, Digest};
use rand::{CryptoRng, Rng};
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, Address, PrivateKey, ViewKey};
use snarkvm_utilities::ToBytes;
use std::convert::TryFrom;
use wasm_bindgen::prelude::*;

pub fn get_authorization_value<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    method: &str,
    path: &str,
    rng: &mut R,
) -> Result<String, JsValue> {
    let view_key = ViewKey::try_from(private_key).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    let address = Address::try_from(private_key)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
        .to_string();

    let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());
    let signature = hex::encode(
        &view_key
            .sign(message.as_bytes(), rng)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?
            .to_bytes_le()
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?,
    );

    let authorization = format!("Aleo {}:{}", address, signature);
    Ok(authorization)
}

pub fn sign_contribution_state<R: Rng + CryptoRng>(
    signing_key: &PrivateKey<Testnet2Parameters>,
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

    let view_key = ViewKey::try_from(signing_key).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    let signature = hex::encode(
        &view_key
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
