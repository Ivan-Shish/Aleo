#[macro_use]
extern crate serde_derive;

cfg_if::cfg_if! {
    if #[cfg(not(test))] {
        #[cfg(feature = "wasm")]
        mod contributor;
        #[cfg(feature = "wasm")]
        mod errors;
        mod pool;
        #[cfg(feature = "wasm")]
        mod requests;
        #[cfg(feature = "wasm")]
        mod structures;
        #[cfg(feature = "wasm")]
        mod utils;
    }
}

mod phase1;
#[cfg(test)]
mod tests;
