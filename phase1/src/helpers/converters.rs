use crate::ProvingSystem;

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

pub fn proving_system_from_str(src: &str) -> Result<ProvingSystem, String> {
    let system = match src.to_lowercase().as_str() {
        "groth16" => ProvingSystem::Groth16,
        "marlin" => ProvingSystem::Marlin,
        _ => return Err("unsupported proving system. Currently supported: groth16, marlin".to_string()),
    };
    Ok(system)
}
