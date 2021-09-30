use crate::{errors::ContributeError, phase1::Phase1WASM};
// use snarkvm_dpc::Address;
use rand::{CryptoRng, Rng};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// Inner ceremony parameters.
const CURVE_KIND: &'static str = "bls12_377";
const PROVING_SYSTEM: &'static str = "groth16";
const BATCH_SIZE: usize = 10000;
const POWER: usize = 19;

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
    unimplemented!();
}

async fn attempt_contribution<R: Rng + CryptoRng>(server_url: String, rng: &mut R) -> Result<bool, JsValue> {
    unimplemented!();
}
