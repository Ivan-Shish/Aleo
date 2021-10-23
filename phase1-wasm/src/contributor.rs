use crate::{phase1::Phase1WASM, pool::WorkerProcess, requests::*, utils::*};
use rand::{CryptoRng, Rng};
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, PrivateKey};
use std::{str::FromStr, time::Duration};
use wasm_bindgen::prelude::*;

// Inner ceremony parameters.
const CURVE_KIND: &'static str = "bls12_377";
const PROVING_SYSTEM: &'static str = "groth16";
const BATCH_SIZE: usize = 2097152;
const POWER: usize = 19;
const CHALLENGE_SIZE: usize = 32768;

const DELAY_FAILED_UPLOAD: Duration = Duration::from_secs(5);
const DELAY_IN_QUEUE: Duration = Duration::from_secs(30);

#[wasm_bindgen]
pub async fn contribute(server_url: String, private_key: String, confirmation_key: String) -> Result<JsValue, JsValue> {
    let mut rng = rand::thread_rng();
    let private_key = PrivateKey::from_str(&private_key).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    join_queue(&private_key, &confirmation_key, server_url.clone(), &mut rng).await?;
    let worker_pool = WorkerProcess::new(8)?;

    loop {
        send_heartbeat(&private_key, server_url.clone(), &mut rng).await?;
        let is_finished = attempt_contribution(&private_key, server_url.clone(), &mut rng, &worker_pool).await?;

        if is_finished {
            break;
        }
    }

    Ok("finished!".into())
}

async fn attempt_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    rng: &mut R,
    worker_pool: &WorkerProcess,
) -> Result<bool, JsValue> {
    let tasks_left = match tasks_left(private_key, server_url.clone(), rng).await {
        Ok(b) => b,
        Err(_) => return Ok(false),
    };

    if !tasks_left {
        return Ok(true);
    }

    web_sys::console::log_1(&"locking chunk".into());
    let response = lock_chunk(private_key, server_url.clone(), rng).await?;
    web_sys::console::log_1(&"chunk locked".into());

    web_sys::console::log_1(&"downloading challenge".into());
    let chunk_bytes = download_challenge(
        private_key,
        server_url.clone(),
        response.chunk_id,
        response.contribution_id,
        rng,
    )
    .await?;
    web_sys::console::log_1(&"challenge downloaded".into());
    web_sys::console::log_1(&format!("{} bytes", chunk_bytes.len()).into());

    let seed: [u8; 32] = rng.gen();

    web_sys::console::log_1(&"contributing...".into());
    let result = Phase1WASM::contribute_chunked(
        CURVE_KIND,
        PROVING_SYSTEM,
        BATCH_SIZE,
        POWER,
        response.chunk_id as usize,
        CHALLENGE_SIZE,
        &seed,
        chunk_bytes.to_vec(),
        &worker_pool,
    )
    .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
    web_sys::console::log_1(&"finished!".into());

    web_sys::console::log_1(&"calculating hashes".into());
    let challenge_hash = calculate_hash(&chunk_bytes);
    let response_hash = calculate_hash(&result.response);

    web_sys::console::log_1(&"signing contribution".into());
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

    web_sys::console::log_1(&"uploading".into());
    loop {
        match upload_response(
            private_key,
            server_url.clone(),
            response.chunk_id,
            response.contribution_id + 1,
            sig_and_result_bytes.clone(),
            rng,
        )
        .await
        {
            Ok(_) => break,
            Err(_e) => {}
        }
    }

    web_sys::console::log_1(&"notifying".into());
    loop {
        match notify_contribution(private_key, server_url.clone(), response.chunk_id, rng).await {
            Ok(_) => break,
            Err(_e) => {}
        }
    }

    Ok(false)
}
