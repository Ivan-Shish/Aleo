use crate::utils::*;
use rand::{CryptoRng, Rng};
use setup1_shared::structures::LockResponse;
use snarkvm_dpc::{testnet2::Testnet2, PrivateKey};
use wasm_bindgen::prelude::*;

pub async fn join_queue<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    confirmation_key: &str,
    server_url: String,
    rng: &mut R,
) -> Result<bool, JsValue> {
    let join_queue_path = "/v1/queue/contributor/join";
    let mut join_queue_url = server_url.clone();
    join_queue_url.push_str(&join_queue_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "POST", &join_queue_path, rng)?;

    let bytes = serde_json::to_vec(confirmation_key).map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    let response = client
        .post(&join_queue_url)
        .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_LENGTH, bytes.len())
        .body(bytes)
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

pub async fn send_heartbeat<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    server_url: String,
    rng: &mut R,
) -> Result<(), JsValue> {
    let heartbeat_path = "/v1/contributor/heartbeat";
    let mut heartbeat_url = server_url.clone();
    heartbeat_url.push_str(&heartbeat_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "POST", &heartbeat_path, rng)?;

    let response = client
        .post(&heartbeat_url)
        .header(http::header::AUTHORIZATION, authorization)
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

pub async fn tasks_left<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    server_url: String,
    rng: &mut R,
) -> Result<bool, JsValue> {
    let task_path = "/v1/contributor/get_task";
    let mut task_url = server_url.clone();
    task_url.push_str(&task_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "POST", &task_path, rng)?;

    let response = client
        .post(&task_url)
        .header(http::header::AUTHORIZATION, authorization)
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

pub async fn lock_chunk<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    server_url: String,
    rng: &mut R,
) -> Result<LockResponse, JsValue> {
    let lock_path = "/v1/contributor/try_lock";
    let mut lock_url = server_url.clone();
    lock_url.push_str(&lock_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "POST", &lock_path, rng)?;

    let response = client
        .post(&lock_url)
        .header(http::header::AUTHORIZATION, authorization)
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

pub async fn download_challenge<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    rng: &mut R,
) -> Result<Vec<u8>, JsValue> {
    let download_path = format!("/v1/download/challenge/{}/{}", chunk_id, contribution_id);
    let mut download_url = server_url.clone();
    download_url.push_str(&download_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "GET", &download_path, rng)?;

    let response = client
        .post(&download_url)
        .header(http::header::AUTHORIZATION, authorization)
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

pub async fn upload_response<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
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
    let authorization = get_authorization_value(private_key, "POST", &upload_path, rng)?;

    let _response = client
        .post(&upload_url)
        .header(http::header::AUTHORIZATION, authorization)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(http::header::CONTENT_LENGTH, sig_and_result_bytes.len())
        .send()
        .await
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?
        .error_for_status()
        .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

    Ok(())
}

pub async fn notify_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2>,
    server_url: String,
    chunk_id: u64,
    rng: &mut R,
) -> Result<(), JsValue> {
    let contribute_path = format!("/v1/contributor/try_contribute/{}", chunk_id);
    let mut contribute_url = server_url.clone();
    contribute_url.push_str(&contribute_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "POST", &contribute_path, rng)?;

    let response = client
        .post(&contribute_url)
        .header(http::header::AUTHORIZATION, authorization)
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
