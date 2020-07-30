use powersoftau::parameters::CeremonyParams;
use snark_utils::UseCompression;
use snarkos_curves::bls12_377::Bls12_377 as AleoBls12_377;
use snarkos_models::curves::{PairingEngine as AleoPairingEngine, AffineCurve as AleoAffineCurve};
use snarkos_utilities::serialize::*;
use test_helpers::setup_verify;
use zexe_algebra::{Bls12_377 as ZexeBls12_377, PairingEngine, AffineCurve, CanonicalDeserialize as ZexeCanonicalDeserialize};

#[test]
fn interop_test() {
    aleo_interoperable_powersoftau::<AleoBls12_377, ZexeBls12_377>().unwrap();
    // aleo_interoperable_powersoftau::<AleoBW6, ZexeBW6>();
}

fn aleo_interoperable_powersoftau<Aleo: AleoPairingEngine, Zexe: PairingEngine>(
) -> anyhow::Result<()> {
    // Generate an accumulator via Zexe's trusted setup:
    let (powers, batch) = (6, 4);
    let params = CeremonyParams::<Zexe>::new(powers, batch);
    let compressed = UseCompression::No;
    // make 1 power of tau contribution (assume powers of tau gets calculated properly)
    let (_, output, _, _) = setup_verify(compressed, compressed, &params);
    let output = std::io::Cursor::new(output);
    let mut reader = std::io::BufReader::new(output);

    // The accumulator is structured as:
    // Vec<G1>, Vec<G2>, Vec<G1>, Vec<G1>, G2
    // tau, tau, alpha, beta, beta

    fn check_compat<AleoCurve: AleoAffineCurve, ZexeCurve: AffineCurve, R: Read>(reader: &mut R) -> anyhow::Result<()> {
        let mut buf = vec![0; AleoCurve::UNCOMPRESSED_SIZE];
        reader.read_exact(&mut buf)?;
        let aleo_g: AleoCurve = CanonicalDeserialize::deserialize_uncompressed(&mut std::io::Cursor::new(&buf))?;
        let mut aleo_buf = vec![];
        aleo_g.write(&mut aleo_buf)?;

        let zexe_g: ZexeCurve = ZexeCanonicalDeserialize::deserialize_uncompressed(&mut std::io::Cursor::new(&buf))?;
        let mut zexe_buf = vec![];
        zexe_g.write(&mut zexe_buf)?;
        assert_eq!(aleo_buf, zexe_buf);

        Ok(())
    }

    let mut hash = vec![0; params.hash_size];
    reader.read_exact(&mut hash)?;
    for _ in 0..params.powers_g1_length {
        check_compat::<Aleo::G1Affine, Zexe::G1Affine, _>(&mut reader)?;
    }
    for _ in 0..params.powers_length {
        check_compat::<Aleo::G2Affine, Zexe::G2Affine, _>(&mut reader)?;
    }
    for _ in 0..params.powers_length {
        check_compat::<Aleo::G1Affine, Zexe::G1Affine, _>(&mut reader)?;
    }
    for _ in 0..params.powers_length {
        check_compat::<Aleo::G1Affine, Zexe::G1Affine, _>(&mut reader)?;
    }
    check_compat::<Aleo::G2Affine, Zexe::G2Affine, _>(&mut reader)?;

    Ok(())
}
