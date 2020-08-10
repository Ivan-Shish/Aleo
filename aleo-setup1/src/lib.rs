// Documentation
#![cfg_attr(nightly, feature(doc_cfg, external_doc))]
#![cfg_attr(nightly, doc(include = "../README.md"))]

pub mod cli;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
