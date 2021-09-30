use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[derive(Clone, Copy, Debug)]
pub enum ContributeError {
    FailedToJoinQueue,
}

impl From<ContributeError> for JsValue {
    fn from(value: ContributeError) -> Self {
        JsValue::from_str(&format!("{:?}", value))
    }
}
