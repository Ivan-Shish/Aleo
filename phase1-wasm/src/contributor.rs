use crate::{
    phase1::{Phase1WASM, Settings},
    pool::WorkerProcess,
    requests::*,
    utils::*,
};
use js_sys::{Function, Promise};
use rand::{CryptoRng, Rng};
use setup1_shared::structures::{LockResponse, PublicSettings, SetupKind};
use snarkvm_dpc::{parameters::testnet2::Testnet2Parameters, PrivateKey};
use std::str::FromStr;
use url::Url;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// Inner ceremony parameters
const INNER_SETTINGS: Settings = Settings {
    curve_kind: "bls12_377",
    proving_system: "groth16",
    batch_size: 2097152,
    power: 19,
    chunk_size: 32768,
};

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
    let server_url = Url::parse(&server_url).map_err(map_js_err)?;
    let mut rng = rand::thread_rng();
    let private_key = PrivateKey::from_str(&private_key).map_err(map_js_err_dbg)?;

    let public_settings = request_coordinator_public_settings_retry(&server_url).await?;

    if public_settings.check_reliability {
        return Err(map_js_err(anyhow::anyhow!(
            "Reliability checks are unsupported for wasm client"
        )));
    }

    let settings = match public_settings.setup {
        SetupKind::Inner => &INNER_SETTINGS,
        SetupKind::Development => &INNER_SETTINGS,
        _ => {
            return Err(map_js_err(anyhow::anyhow!(
                "Unsupported setup kind: {:?}",
                public_settings.setup
            )));
        }
    };

    join_queue(&private_key, confirmation_key, &server_url, &mut rng)
        .await
        .map_err(map_js_err)?;

    let worker_pool = WorkerProcess::new(DEFAULT_THREAD_COUNT)?;
    let seed: [u8; 32] = rng.gen();

    loop {
        send_heartbeat(&private_key, &server_url, &mut rng).await?;

        let is_finished = attempt_contribution(settings, &private_key, &server_url, &seed, &mut rng, &worker_pool)
            .await
            .map_err(map_js_err)?;

        if is_finished {
            break;
        }
    }

    Ok("finished!".into())
}

async fn attempt_contribution<R: Rng + CryptoRng>(
    settings: &Settings,
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    seed: &[u8],
    rng: &mut R,
    worker_pool: &WorkerProcess,
) -> anyhow::Result<bool> {
    let tasks_left = match get_tasks_left(private_key, server_url, rng).await {
        Ok(b) => b,
        Err(_) => {
            sleep(DELAY_THIRTY_SECONDS)
                .await
                .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            return Ok(false);
        }
    };

    if !tasks_left {
        return Ok(true);
    }

    web_sys::console::log_1(&"locking chunk".into());
    let response = lock_chunk(private_key, server_url, rng).await?;
    web_sys::console::log_1(&"chunk locked".into());

    web_sys::console::log_1(&"downloading challenge".into());
    let chunk_bytes = download_challenge(
        private_key,
        server_url,
        response.chunk_id,
        response.contribution_id,
        rng,
    )
    .await?;
    web_sys::console::log_1(&"challenge downloaded".into());
    web_sys::console::log_1(&format!("{} bytes", chunk_bytes.len()).into());

    web_sys::console::log_1(&"contributing...".into());
    let result = Phase1WASM::contribute_chunked(
        settings,
        response.chunk_id as usize,
        seed,
        chunk_bytes.to_vec(),
        &worker_pool,
        DEFAULT_THREAD_COUNT,
    )?;
    web_sys::console::log_1(&"finished!".into());

    web_sys::console::log_1(&"calculating hashes".into());
    let challenge_hash = calculate_hash(&chunk_bytes);
    let response_hash = calculate_hash(&result.response);

    web_sys::console::log_1(&"signing contribution".into());
    let signed_contribution_state = sign_contribution_state(&private_key, &challenge_hash, &response_hash, None, rng)?;
    let verifier_flag = vec![0];
    let signature_bytes = hex::decode(signed_contribution_state.get_signature())?;

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
        server_url,
        response.response_chunk_id,
        response.response_contribution_id,
        sig_and_result_bytes.clone(),
        rng,
    )
    .await?;

    web_sys::console::log_1(&"notifying".into());
    notify_contribution(private_key, server_url, response.chunk_id, rng).await?;

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

async fn request_coordinator_public_settings_retry(server_url: &Url) -> Result<PublicSettings, JsValue> {
    loop {
        match request_coordinator_public_settings(server_url).await {
            Ok(settings) => return Ok(settings),
            Err(e) => {
                web_sys::console::log_1(&format!("Error requesting public settings: {:?}", e).into());
                sleep(DELAY_FIVE_SECONDS).await?;
            }
        }
    }
}

async fn send_heartbeat<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    rng: &mut R,
) -> Result<(), JsValue> {
    loop {
        match post_heartbeat(private_key, server_url, rng).await {
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
    server_url: &Url,
    rng: &mut R,
) -> anyhow::Result<()> {
    loop {
        match post_join_queue(&private_key, &confirmation_key, server_url, rng).await {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_THIRTY_SECONDS)
                    .await
                    .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            }
        };
    }

    Ok(())
}

async fn lock_chunk<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    rng: &mut R,
) -> anyhow::Result<LockResponse> {
    let response;

    loop {
        match post_lock_chunk(private_key, server_url, rng).await {
            Ok(r) => {
                response = r;
                break;
            }
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS)
                    .await
                    .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            }
        };
    }

    Ok(response)
}

async fn download_challenge<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    chunk_id: u64,
    contribution_id: u64,
    rng: &mut R,
) -> anyhow::Result<Vec<u8>> {
    let chunk_bytes;

    loop {
        match get_challenge(private_key, server_url, chunk_id, contribution_id, rng).await {
            Ok(c) => {
                chunk_bytes = c;
                break;
            }
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS)
                    .await
                    .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            }
        }
    }

    Ok(chunk_bytes)
}

async fn upload_response<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    chunk_id: u64,
    contribution_id: u64,
    sig_and_result_bytes: Vec<u8>,
    rng: &mut R,
) -> anyhow::Result<()> {
    loop {
        match post_response(
            private_key,
            server_url,
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
                sleep(DELAY_FIVE_SECONDS)
                    .await
                    .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            }
        }
    }

    Ok(())
}

async fn notify_contribution<R: Rng + CryptoRng>(
    private_key: &PrivateKey<Testnet2Parameters>,
    server_url: &Url,
    chunk_id: u64,
    rng: &mut R,
) -> anyhow::Result<()> {
    loop {
        match post_contribution(private_key, server_url, chunk_id, rng).await {
            Ok(_) => break,
            Err(e) => {
                web_sys::console::log_1(&format!("{:?}", e).into());
                sleep(DELAY_FIVE_SECONDS)
                    .await
                    .map_err(|e| anyhow::anyhow!("Error performing sleep: {:?}", e))?;
            }
        }
    }

    Ok(())
}
