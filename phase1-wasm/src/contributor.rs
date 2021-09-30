use crate::{errors::ContributeError, phase1::Phase1WASM};
// use snarkvm_dpc::Address;
use rand::{CryptoRng, Rng};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Contributor {
    wasm: Phase1WASM,
    address: String,
    confirmation_key: String,
}

#[wasm_bindgen]
impl Contributor {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self { wasm: Phase1WASM {} }
    }

    pub async fn contribute(&self) -> Result<JsValue, JsValue> {
        let mut rng = rand::thread_rng();
        self.join_queue(&mut rng).await?;

        loop {
            self.send_heartbeat(&mut rng).await?;
            self.attempt_contribution(&mut rng).await?;

            if self.is_finished(&mut rng).await? {
                break;
            }
        }

        Ok((self.address, self.confirmation_key))
    }

    async fn join_queue<R: Rng + CryptoRng>(&self, rng: &mut R) -> Result<bool, ContributeError> {
        let join_queue_path = "/v1/queue/contributor/join";
        // TODO: finish
        unimplemented!();
    }

    async fn send_heartbeat<R: Rng + CryptoRng>(&self, rng: &mut R) -> Result<(), ContributeError> {
        unimplemented!();
    }

    async fn attempt_contribution<R: Rng + CryptoRng>(&self, rng: &mut R) -> Result<(), ContributeError> {
        unimplemented!();
    }

    async fn is_finished<R: Rng + CryptoRng>(&self, rng: &mut R) -> Result<bool, ContributeError> {
        unimplemented!();
    }
}
