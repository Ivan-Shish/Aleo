use powersoftau::parameters::CeremonyParams;
use snark_utils::UseCompression;
use test_helpers::setup_verify;

use snarkos_curves::{bls12_377::Bls12_377 as AleoBls12_377, bw6_761::BW6_761 as AleoBW6};
use snarkos_models::curves::PairingEngine as AleoPairingEngine;
use snarkos_utilities::{serialize::CanonicalDeserialize as AleoCanonicalDeserialize, ToBytes as AleoToBytes};

use zexe_algebra::{
    Bls12_377 as ZexeBls12_377,
    PairingEngine as ZexePairingEngine,
    ToBytes as ZexeToBytes,
    BW6_761 as ZexeBW6,
};
use zexe_algebra_core::serialize::CanonicalDeserialize as ZexeCanonicalDeserialize;

fn assert_eq_g1_affine<Aleo: AleoPairingEngine, Zexe: ZexePairingEngine>(a: &Aleo::G1Affine, b: &Zexe::G1Affine) {
    let mut a_vec = vec![];
    a.write(&mut a_vec).unwrap();

    let mut b_vec = vec![];
    b.write(&mut b_vec).unwrap();

    assert_eq!(a_vec.len(), b_vec.len());
    for (a_byte, b_byte) in a_vec.iter().zip(b_vec.iter()) {
        assert_eq!(a_byte, b_byte);
    }
}

fn assert_eq_g2_affine<Aleo: AleoPairingEngine, Zexe: ZexePairingEngine>(a: &Aleo::G2Affine, b: &Zexe::G2Affine) {
    let mut a_vec = vec![];
    a.write(&mut a_vec).unwrap();

    let mut b_vec = vec![];
    b.write(&mut b_vec).unwrap();

    assert_eq!(a_vec.len(), b_vec.len());
    for (a_byte, b_byte) in a_vec.iter().zip(b_vec.iter()) {
        assert_eq!(a_byte, b_byte);
    }
}

fn compatible_powersoftau<Aleo: AleoPairingEngine, Zexe: ZexePairingEngine>() -> anyhow::Result<()> {
    // Generate an accumulator via Zexe's trusted setup, and clone a copy for Aleo to parse for comparison.
    let (output_for_aleo, output_for_zexe) = {
        let (powers, batch) = (6, 4);
        let params = CeremonyParams::<Zexe>::new(powers, batch);

        // Perform 1 power of tau contribution (assume Powers of Tau is computed correctly)
        let compressed = UseCompression::No;
        let (_, output, _, _) = setup_verify(compressed, compressed, &params);

        (std::io::Cursor::new(output.clone()), std::io::Cursor::new(output))
    };

    // Deserialize into the group elements using Aleo's infrastructure
    let (aleo_tau_g1, aleo_tau_g2, aleo_alpha_g1, aleo_beta_g1, aleo_beta_g2) = {
        let mut aleo_reader = std::io::BufReader::new(output_for_aleo);

        // The accumulator is structured as:
        // Vec<G1>, Vec<G2>, Vec<G1>, Vec<G1>, G2
        // tau, tau, alpha, beta, beta
        let tau_g1: Vec<Aleo::G1Affine> = AleoCanonicalDeserialize::deserialize(&mut aleo_reader)?;
        let tau_g2: Vec<Aleo::G2Affine> = AleoCanonicalDeserialize::deserialize(&mut aleo_reader)?;
        let alpha_g1: Vec<Aleo::G1Affine> = AleoCanonicalDeserialize::deserialize(&mut aleo_reader)?;
        let beta_g1: Vec<Aleo::G1Affine> = AleoCanonicalDeserialize::deserialize(&mut aleo_reader)?;
        let beta_g2: Aleo::G2Affine = AleoCanonicalDeserialize::deserialize(&mut aleo_reader)?;

        (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2)
    };

    // Deserialize into the group elements using Zexe's infrastructure
    let (zexe_tau_g1, zexe_tau_g2, zexe_alpha_g1, zexe_beta_g1, zexe_beta_g2) = {
        let mut zexe_reader = std::io::BufReader::new(output_for_zexe);

        // The accumulator is structured as:
        // Vec<G1>, Vec<G2>, Vec<G1>, Vec<G1>, G2
        // tau, tau, alpha, beta, beta
        let tau_g1: Vec<Zexe::G1Affine> = ZexeCanonicalDeserialize::deserialize(&mut zexe_reader)?;
        let tau_g2: Vec<Zexe::G2Affine> = ZexeCanonicalDeserialize::deserialize(&mut zexe_reader)?;
        let alpha_g1: Vec<Zexe::G1Affine> = ZexeCanonicalDeserialize::deserialize(&mut zexe_reader)?;
        let beta_g1: Vec<Zexe::G1Affine> = ZexeCanonicalDeserialize::deserialize(&mut zexe_reader)?;
        let beta_g2: Zexe::G2Affine = ZexeCanonicalDeserialize::deserialize(&mut zexe_reader)?;

        (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2)
    };

    // Verify that the outputs are equivalent.

    assert_eq!(aleo_tau_g1.len(), zexe_tau_g1.len());
    assert_eq!(aleo_tau_g2.len(), zexe_tau_g2.len());
    assert_eq!(aleo_alpha_g1.len(), zexe_alpha_g1.len());
    assert_eq!(aleo_beta_g1.len(), zexe_beta_g1.len());

    for (a, b) in aleo_tau_g1.iter().zip(zexe_tau_g1.iter()) {
        assert_eq_g1_affine::<Aleo, Zexe>(a, b);
    }
    for (a, b) in aleo_tau_g2.iter().zip(zexe_tau_g2.iter()) {
        assert_eq_g2_affine::<Aleo, Zexe>(a, b);
    }
    for (a, b) in aleo_alpha_g1.iter().zip(zexe_alpha_g1.iter()) {
        assert_eq_g1_affine::<Aleo, Zexe>(a, b);
    }
    for (a, b) in aleo_beta_g1.iter().zip(zexe_beta_g1.iter()) {
        assert_eq_g1_affine::<Aleo, Zexe>(a, b);
    }

    assert_eq_g2_affine::<Aleo, Zexe>(&aleo_beta_g2, &zexe_beta_g2);

    Ok(())
}

#[test]
fn test_aleo_zexe_bls12_377_compatibility() {
    compatible_powersoftau::<AleoBls12_377, ZexeBls12_377>().unwrap();
}

#[test]
fn test_aleo_zexe_bw6_761_compatibility() {
    compatible_powersoftau::<AleoBW6, ZexeBW6>().unwrap();
}
