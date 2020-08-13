// Documentation
#![cfg_attr(nightly, feature(doc_cfg, external_doc))]
#![cfg_attr(nightly, doc(include = "../README.md"))]

#[cfg(target_arch = "wasm32")]
#[macro_use]
extern crate serde_derive;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[derive(Debug, Clone)]
pub enum CurveKind {
    Bls12_377,
    BW6,
}

pub fn curve_from_str(src: &str) -> Result<CurveKind, String> {
    let curve = match src.to_lowercase().as_str() {
        "bls12_377" => CurveKind::Bls12_377,
        "bw6" => CurveKind::BW6,
        _ => return Err("unsupported curve".to_string()),
    };
    Ok(curve)
}
