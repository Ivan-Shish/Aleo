use crate::{errors::ContributeError, phase1::Phase1WASM};
// use snarkvm_dpc::Address;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Contributor {
    wasm: Phase1WASM,
}

impl Contributor {
    pub fn contribute() -> Result<String, ContributeError> {
        Ok("1".to_owned())
    }
}
