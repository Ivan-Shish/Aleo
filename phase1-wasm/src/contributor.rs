use crate::{errors::ContributeError, phase1::Phase1WASM};
// use snarkvm_dpc::Address;
use rand::{CryptoRng, Rng};
use std::time::Duration;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// Inner ceremony parameters.
const CURVE_KIND: &'static str = "bls12_377";
const PROVING_SYSTEM: &'static str = "groth16";
const BATCH_SIZE: usize = 10000;
const POWER: usize = 19;

const DELAY_FAILED_UPLOAD: Duration = Duration::from_secs(5);

#[wasm_bindgen]
pub async fn contribute(server_url: String) -> Result<JsValue, JsValue> {
    let mut rng = rand::thread_rng();

    // TODO: generate keys and stuff

    join_queue(server_url.clone(), &mut rng).await?;

    loop {
        send_heartbeat(server_url.clone(), &mut rng).await?;
        let is_finished = attempt_contribution(server_url.clone(), &mut rng).await?;

        if is_finished {
            break;
        }
    }

    unimplemented!();
    // Ok(
    //     JsValue::from_serde(&(self.address.clone(), self.confirmation_key.clone()))
    //         .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?,
    // )
}

async fn join_queue<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<bool, JsValue> {
    let join_queue_path = "/v1/queue/contributor/join";
    let mut join_queue_url = server_url.clone();
    join_queue_url.push_str(&join_queue_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    // TODO: get conf key

    let response = client
            .post(&join_queue_url)
            // .header(http::header::AUTHORIZATION, authorization)
            // .header(http::header::CONTENT_LENGTH, bytes.len())
            // .body(bytes)
            .send()
            .await
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?
            .error_for_status()
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let data = response
        .bytes()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
    let joined = serde_json::from_slice::<bool>(&*data).map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(joined)
}

async fn send_heartbeat<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<(), JsValue> {
    let heartbeat_path = "/v1/contributor/heartbeat";
    let mut heartbeat_url = server_url.clone();
    heartbeat_url.push_str(&heartbeat_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&heartbeat_url)
        // .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_LENGTH, 0)
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    response
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(())
}

async fn attempt_contribution<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<bool, JsValue> {
    if !tasks_left(server_url.clone(), rng).await? {
        return Ok(true);
    }

    let response = lock_chunk(server_url.clone(), rng).await?;
    let chunk_bytes = download_challenge(server_url.clone(), response.chunk_id, response.contribution_id, rng).await?;

    let seed: [u8; 32] = rng.gen();

    let result = Phase1WASM::contribute_chunked(
        CURVE_KIND,
        PROVING_SYSTEM,
        BATCH_SIZE,
        POWER,
        response.chunk_id,
        chunk_bytes.len(),
        &seed,
        chunk_bytes,
    )?
    .into_serde()?;

    loop {
        match upload_response(
            response.chunk_id,
            response.contribution_id,
            sig_and_result_bytes.clone(),
            rng,
        )
        .await
        {
            Ok(_) => break,
            Err(_e) => std::thread::sleep(DELAY_FAILED_UPLOAD),
        }
    }

    loop {
        match notify_contribution(response.chunk_id, rng).await {
            Ok(_) => break,
            Err(_e) => std::thread::sleep(DELAY_FAILED_UPLOAD),
        }
    }

    Ok(false)
}
