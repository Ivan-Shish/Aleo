use crate::{errors::ContributeError, phase1::Phase1WASM};
// use snarkvm_dpc::Address;
use rand::{CryptoRng, Rng};
use setup1_shared::structures::LockResponse;
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
        response.chunk_id as usize,
        chunk_bytes.len(),
        &seed,
        &chunk_bytes,
    )?
    .into_serde()
    .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    // TODO: create signature here

    loop {
        match upload_response(
            server_url.clone(),
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
        match notify_contribution(server_url.clone(), response.chunk_id, rng).await {
            Ok(_) => break,
            Err(_e) => std::thread::sleep(DELAY_FAILED_UPLOAD),
        }
    }

    Ok(false)
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

async fn tasks_left<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<bool, JsValue> {
    let task_path = "/v1/contributor/get_task";
    let mut task_url = server_url.clone();
    task_url.push_str(&task_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&task_url)
        // .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_LENGTH, 0)
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let data = response
        .bytes()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
    let tasks_left = serde_json::from_slice::<bool>(&*data).map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(tasks_left)
}

async fn lock_chunk<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<LockResponse, JsValue> {
    let lock_path = "/v1/contributor/try_lock";
    let mut lock_url = server_url.clone();
    lock_url.push_str(&lock_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&lock_url)
        // .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_LENGTH, 0)
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let data = response
        .bytes()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
    let lock_response =
        serde_json::from_slice::<LockResponse>(&*data).map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(lock_response)
}

async fn download_challenge<R: Rng + CryptoRng>(
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    rng: &mut R,
) -> Result<Vec<u8>, JsValue> {
    let download_path = format!("/v1/download/challenge/{}/{}", chunk_id, contribution_id);
    let mut download_url = server_url.clone();
    download_url.push_str(&download_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&download_url)
        // .header(http::header::AUTHORIZATION, authorization)
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let chunk_bytes = response
        .bytes()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(chunk_bytes.to_vec())
}

async fn upload_response<R: Rng + CryptoRng>(
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    sig_and_result_bytes: &[u8],
    rng: &mut R,
) -> Result<(), JsValue> {
    let upload_path = format!("/v1/upload/response/{}/{}", chunk_id, contribution_id);
    let mut upload_url = server_url.clone();
    upload_url.push_str(&upload_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&upload_url)
        // .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(http::header::CONTENT_LENGTH, sig_and_result_bytes.len())
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(())
}

async fn notify_contribution<R: Rng + CryptoRng>(
    server_url: String,
    chunk_id: u64,
    rng: &mut R,
) -> Result<(), JsValue> {
    let contribute_path = format!("/v1/contributor/try_contribute/{}", chunk_id);
    let mut contribute_url = server_url.clone();
    contribute_url.push_str(&contribute_path);
    let client = reqwest::Client::new();
    // TODO: get auth

    let response = client
        .post(&contribute_url)
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
