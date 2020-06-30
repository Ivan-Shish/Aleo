use snarkos_utilities::serialize::*;
use snarkos_models::curves::PairingEngine as AleoPairingEngine;
use snarkos_curves::bls12_377::Bls12_377 as AleoBls12_377;
use test_helpers::setup_verify;
use snark_utils::UseCompression;
use powersoftau::{parameters::CeremonyParams, BatchedAccumulator};
use zexe_algebra::{Bls12_377 as CeloBls12_377, PairingEngine};

#[test]
fn interop_test() {
    aleo_interoperable_powersoftau::<AleoBls12_377, CeloBls12_377>();
    // aleo_interoperable_powersoftau::<AleoBW6, CeloBW6>();
}

fn aleo_interoperable_powersoftau<Aleo: AleoPairingEngine, Celo: PairingEngine>() -> anyhow::Result<()> {
    // Generate an accumulator via Celo's trusted setup:
    let (powers, batch) = (6, 4);
    let params = CeremonyParams::<Celo>::new(powers, batch);
    let compressed = UseCompression::No;
    // make 1 power of tau contribution (assume powers of tau gets calculated properly)
    let (_, output, _, _) = setup_verify(compressed, compressed, &params);
    let output = std::io::Cursor::new(output);
    let mut reader = std::io::BufReader::new(output);

    // The accumulator is structured as:
    // Vec<G1>, Vec<G2>, Vec<G1>, Vec<G1>, G2
    // tau, tau, alpha, beta, beta
    let tau_g1: Vec<Aleo::G1Affine> = CanonicalDeserialize::deserialize(&mut reader)?;
    let tau_g2: Vec<Aleo::G2Affine> = CanonicalDeserialize::deserialize(&mut reader)?;
    let alpha_g1: Vec<Aleo::G1Affine> = CanonicalDeserialize::deserialize(&mut reader)?;
    let beta_g1: Vec<Aleo::G1Affine> = CanonicalDeserialize::deserialize(&mut reader)?;
    let beta_g2: Aleo::G1Affine = CanonicalDeserialize::deserialize(&mut reader)?;

    Ok(())
}
