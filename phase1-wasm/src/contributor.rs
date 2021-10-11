use crate::{phase1::Phase1WASM, requests::*, utils::*};
use rand::{CryptoRng, Rng};
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, Address, PrivateKey};
use std::time::Duration;
use wasm_bindgen::prelude::*;

// Inner ceremony parameters.
const CURVE_KIND: &'static str = "bls12_377";
const PROVING_SYSTEM: &'static str = "groth16";
const BATCH_SIZE: usize = 10000;
const POWER: usize = 19;

const DELAY_FAILED_UPLOAD: Duration = Duration::from_secs(5);

#[wasm_bindgen]
pub async fn contribute(server_url: String) -> Result<JsValue, JsValue> {
    let mut rng = rand::thread_rng();
    let private_key = PrivateKey::new(&mut rng);
    let address = Address::from_private_key(&private_key).expect("Should have derived an Aleo address");
    let confirmation_key = generate_confirmation_key(&address, &private_key);

    join_queue(&private_key, &confirmation_key, server_url.clone(), &mut rng).await?;

    loop {
        send_heartbeat(&private_key, server_url.clone(), &mut rng).await?;
        let is_finished = attempt_contribution(&private_key, server_url.clone(), &mut rng).await?;

        if is_finished {
            break;
        }
    }

    Ok(JsValue::from_serde(&(address.to_string(), confirmation_key))
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?)
}

async fn attempt_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    rng: &mut R,
) -> Result<bool, JsValue> {
    if !tasks_left(private_key, server_url.clone(), rng).await? {
        return Ok(true);
    }

    let response = lock_chunk(private_key, server_url.clone(), rng).await?;
    let chunk_bytes = download_challenge(
        private_key,
        server_url.clone(),
        response.chunk_id,
        response.contribution_id,
        rng,
    )
    .await?;

    let seed: [u8; 32] = rng.gen();

    let result = Phase1WASM::contribute_chunked(
        CURVE_KIND,
        PROVING_SYSTEM,
        BATCH_SIZE,
        POWER,
        response.chunk_id as usize,
        chunk_bytes.len(),
        &seed,
        &chunk_bytes,
    )
    .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let challenge_hash = calculate_hash(&chunk_bytes);
    let response_hash = calculate_hash(&result.response);

    let signed_contribution_state = sign_contribution_state(&private_key, &challenge_hash, &response_hash, None, rng)?;
    let verifier_flag = vec![0];
    let signature_bytes =
        hex::decode(signed_contribution_state.get_signature()).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

    let sig_and_result_bytes = [
        verifier_flag,
        signature_bytes,
        challenge_hash,
        response_hash,
        result.response,
    ]
    .concat();

    loop {
        match upload_response(
            private_key,
            server_url.clone(),
            response.chunk_id,
            response.contribution_id,
            &sig_and_result_bytes.clone(),
            rng,
        )
        .await
        {
            Ok(_) => break,
            Err(_e) => std::thread::sleep(DELAY_FAILED_UPLOAD),
        }
    }

    loop {
        match notify_contribution(private_key, server_url.clone(), response.chunk_id, rng).await {
            Ok(_) => break,
            Err(_e) => std::thread::sleep(DELAY_FAILED_UPLOAD),
        }
    }

    Ok(false)
}
