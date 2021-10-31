use crate::{phase1::Phase1WASM, pool::WorkerProcess, requests::*, utils::*};
use js_sys::{Function, Promise};
use rand::{CryptoRng, Rng};
use setup1_shared::structures::LockResponse;
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, PrivateKey};
use std::str::FromStr;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// Inner ceremony parameters.
const CURVE_KIND: &'static str = "bls12_377";
const PROVING_SYSTEM: &'static str = "groth16";
const BATCH_SIZE: usize = 2097152;
const POWER: usize = 19;
const CHALLENGE_SIZE: usize = 32768;

const DELAY_FIVE_SECONDS: i32 = 5000;
const DELAY_THIRTY_SECONDS: i32 = 30000;
const DEFAULT_THREAD_COUNT: usize = 8;

// A custom binding to the JS `setTimeout` function, in order to implement a sleep
// function.
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_timeout(f: &Function, time_ms: i32);
}

/// Performs a full ceremony round, and returns only when all chunks have been
/// contributed to. Takes in a coordinator URL, an Aleo private key and a
/// hash of the confirmation key.
#[wasm_bindgen]
pub async fn contribute(server_url: String, private_key: String, confirmation_key: String) -> Result<JsValue, JsValue> {
    let mut rng = rand::thread_rng();
    let private_key = PrivateKey::from_str(&private_key).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

    join_queue(&private_key, confirmation_key, server_url.clone(), &mut rng).await?;
    let worker_pool = WorkerProcess::new(DEFAULT_THREAD_COUNT)?;
    let seed: [u8; 32] = rng.gen();

    loop {
        send_heartbeat(&private_key, server_url.clone(), &mut rng).await?;

        let is_finished = attempt_contribution(&private_key, server_url.clone(), &seed, &mut rng, &worker_pool).await?;

        if is_finished {
            break;
        }
    }

    Ok("finished!".into())
}

async fn attempt_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    seed: &[u8],
    rng: &mut R,
    worker_pool: &WorkerProcess,
) -> Result<bool, JsValue> {
    let tasks_left = match get_tasks_left(private_key, server_url.clone(), rng).await {
        Ok(b) => b,
        Err(_) => {
            sleep(DELAY_THIRTY_SECONDS).await?;
            return Ok(false);
        }
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

    web_sys::console::log_1(&"contributing...".into());
    let result = Phase1WASM::contribute_chunked(
        CURVE_KIND,
        PROVING_SYSTEM,
        BATCH_SIZE,
        POWER,
        response.chunk_id as usize,
        CHALLENGE_SIZE,
        seed,
        chunk_bytes.to_vec(),
        &worker_pool,
        DEFAULT_THREAD_COUNT,
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
    upload_response(
        private_key,
        server_url.clone(),
        response.response_chunk_id,
        response.response_contribution_id,
        sig_and_result_bytes.clone(),
        rng,
    )
    .await?;

    web_sys::console::log_1(&"notifying".into());
    notify_contribution(private_key, server_url.clone(), response.chunk_id, rng).await?;

    Ok(false)
}

async fn sleep(time_ms: i32) -> Result<(), JsValue> {
    JsFuture::from(Promise::new(&mut |yes, _| {
        set_timeout(&yes, time_ms);
    }))
    .await?;
    Ok(())
}

////////////////////////////////////////////////////////
/// Fault-tolerant wrappers for coordinator requests ///
////////////////////////////////////////////////////////

async fn send_heartbeat<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    rng: &mut R,
) -> Result<(), JsValue> {
    loop {
        match post_heartbeat(private_key, server_url.clone(), rng).await {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_THIRTY_SECONDS).await?;
            }
        };
    }

    Ok(())
}

async fn join_queue<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    confirmation_key: String,
    server_url: String,
    rng: &mut R,
) -> Result<(), JsValue> {
    loop {
        match post_join_queue(&private_key, &confirmation_key, server_url.clone(), rng).await {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_THIRTY_SECONDS).await?;
            }
        };
    }

    Ok(())
}

async fn lock_chunk<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    rng: &mut R,
) -> Result<LockResponse, JsValue> {
    let response;

    loop {
        match post_lock_chunk(private_key, server_url.clone(), rng).await {
            Ok(r) => {
                response = r;
                break;
            }
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS).await?;
            }
        };
    }

    Ok(response)
}

async fn download_challenge<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    rng: &mut R,
) -> Result<Vec<u8>, JsValue> {
    let chunk_bytes;

    loop {
        match get_challenge(private_key, server_url.clone(), chunk_id, contribution_id, rng).await {
            Ok(c) => {
                chunk_bytes = c;
                break;
            }
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS).await?;
            }
        }
    }

    Ok(chunk_bytes)
}

async fn upload_response<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    chunk_id: u64,
    contribution_id: u64,
    sig_and_result_bytes: Vec<u8>,
    rng: &mut R,
) -> Result<(), JsValue> {
    loop {
        match post_response(
            private_key,
            server_url.clone(),
            chunk_id,
            contribution_id,
            sig_and_result_bytes.clone(),
            rng,
        )
        .await
        {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS).await?;
            }
        }
    }

    Ok(())
}

async fn notify_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: String,
    chunk_id: u64,
    rng: &mut R,
) -> Result<(), JsValue> {
    loop {
        match post_contribution(private_key, server_url.clone(), chunk_id, rng).await {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS).await?;
            }
        }
    }

    Ok(())
}
