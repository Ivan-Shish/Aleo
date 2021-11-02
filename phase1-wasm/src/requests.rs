use crate::utils::*;
use js_sys::{Promise, Uint8Array};
use rand::{CryptoRng, Rng};
use setup1_shared::structures::LockResponse;
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, PrivateKey};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

const MAJOR: u8 = 0;
const MINOR: u8 = 1;
const PATCH: u8 = 0;

// A custom binding to the JS `fetch` function, which we use in place of `reqwest`
// in cases where request payload may be malformed.
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = fetch)]
    fn fetch_with_request(input: &web_sys::Request) -> Promise;
}

/// Join the ceremony queue.
///
/// NOTE: This function makes use of the custom binding to `fetch`, since reqwest
/// tends to malform payload blobs. The custom binding bypasses reqwest's
/// serialization procedures and allows us to deliver the payload properly.
pub async fn post_join_queue<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    confirmation_key: &str,
    server_url: String,
    rng: &mut R,
) -> Result<bool, JsValue> {
    let join_queue_path = format!("/v1/queue/contributor/join/{}/{}/{}", MAJOR, MINOR, PATCH);
    let mut join_queue_url = server_url.clone();
    join_queue_url.push_str(&join_queue_path);
    let authorization = get_authorization_value(private_key, "POST", &join_queue_path, rng)?;

    let bytes = serde_json::to_vec(confirmation_key).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

    let mut opts = RequestInit::new();
    opts.method("POST");
    opts.mode(RequestMode::Cors);
    opts.body(Some(&js_sys::Uint8Array::from(bytes.as_slice()).into()));

    let request = Request::new_with_str_and_init(&join_queue_url, &opts)?;

    request.headers().set("Authorization", &authorization)?;
    request.headers().set("Content-Length", &format!("{}", bytes.len()))?;
    request.headers().set("Content-Type", "application/json")?;

    let response = JsFuture::from(fetch_with_request(&request)).await?;

    let response: Response = response.dyn_into().unwrap();
    let data = JsFuture::from(response.json()?).await?;
    let joined: bool = data.into_serde().unwrap();
    Ok(joined)
}

/// Send a heartbeat to the coordinator, signaling that the contributor is still
/// online.
pub async fn post_heartbeat<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
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

/// Inquire the coordinator about the existence of any unfinished tasks. Used to
/// gauge when the contributor is finished.
pub async fn get_tasks_left<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    rng: &mut R,
) -> Result<bool, JsValue> {
    let task_path = "/v1/contributor/get_task";
    let mut task_url = server_url.clone();
    task_url.push_str(&task_path);
    let client = reqwest::Client::new();
    let authorization = get_authorization_value(private_key, "GET", &task_path, rng)?;

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

/// Lock a chunk in the ceremony. This should be the first function called when
/// attempting to contribute to a chunk. Once the chunk is locked, it is ready
/// to be downloaded.
pub async fn post_lock_chunk<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
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

/// Download a chunk from the coordinator, which should be contributed to upon
/// receipt.
///
/// NOTE: This function makes use of the custom binding to `fetch`, since reqwest
/// tends to malform response data. The custom binding bypasses reqwest's
/// serialization procedures and allows us to deliver the payload properly.
pub async fn get_challenge<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    rng: &mut R,
) -> Result<Vec<u8>, JsValue> {
    let download_path = format!("/v1/download/challenge/{}/{}", chunk_id, contribution_id);
    let mut download_url = server_url.clone();
    download_url.push_str(&download_path);
    let authorization = get_authorization_value(private_key, "GET", &download_path, rng)?;

    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(&download_url, &opts)?;

    request.headers().set("Authorization", &authorization)?;

    let response = JsFuture::from(fetch_with_request(&request)).await?;

    let response: Response = response.dyn_into().unwrap();
    let chunk_bytes = JsFuture::from(response.array_buffer()?).await?;
    let chunk_bytes = Uint8Array::new(&chunk_bytes);
    Ok(chunk_bytes.to_vec())
}

/// Upload a chunk contribution to the coordinator.
///
/// NOTE: This function makes use of the custom binding to `fetch`, since reqwest
/// tends to malform response upload blobs. The custom binding bypasses reqwest's
/// serialization procedures and allows us to deliver the payload properly.
pub async fn post_response<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    sig_and_result_bytes: Vec<u8>,
    rng: &mut R,
) -> Result<(), JsValue> {
    let upload_path = format!("/v1/upload/response/{}/{}", chunk_id, contribution_id);
    let mut upload_url = server_url.clone();
    upload_url.push_str(&upload_path);
    let authorization = get_authorization_value(private_key, "POST", &upload_path, rng)?;

    let mut opts = RequestInit::new();
    opts.method("POST");
    opts.mode(RequestMode::Cors);
    opts.body(Some(&js_sys::Uint8Array::from(sig_and_result_bytes.as_slice()).into()));

    let request = Request::new_with_str_and_init(&upload_url, &opts)?;

    request.headers().set("Authorization", &authorization)?;
    request
        .headers()
        .set("Content-Length", &format!("{}", sig_and_result_bytes.len()))?;
    request.headers().set("Content-Type", "application/octet-stream")?;

    let _response = JsFuture::from(fetch_with_request(&request)).await?;

    Ok(())
}

/// Notify the coordinator of a finished and uploaded contribution. This will
/// unlock the given chunk and allow the contributor to take on a new task.
pub async fn post_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
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
