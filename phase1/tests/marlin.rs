#[cfg(test)]
mod test {
    use phase1::{helpers::testing::generate_input, Phase1, Phase1Parameters, ProvingSystem};
    use rand::thread_rng;
    use setup_utils::{blank_hash, CheckForCorrectness, UseCompression};

    use snarkos_curves::bls12_377::{Bls12_377, G1Affine, G2Affine};
    use snarkos_models::{
        algorithms::SNARK,
        curves::{AffineCurve, PairingCurve},
    };
    use snarkos_polycommit::kzg10::UniversalParams;
    use snarkos_posw::{txids_to_roots, Marlin, PoswMarlin};
    use snarkos_utilities::serialize::*;
    use std::io::Cursor;
    use zexe_algebra::{
        bls12_377::{G1Affine as ZexeG1Affine, G2Affine as ZexeG2Affine},
        AffineCurve as ZexeAffineCurve,
        Bls12_377 as ZexeBls12_377,
    };

    fn convert_vec_from_zexe_to_snarkos<Zexe: ZexeAffineCurve, Aleo: AffineCurve>(zexe: &[Zexe]) -> Vec<Aleo> {
        let mut aleo = vec![];

        let mut buffer = vec![];
        for p_zexe in zexe {
            p_zexe.serialize(&mut buffer).unwrap();
            let p = Aleo::deserialize(&mut Cursor::new(&buffer)).unwrap();
            aleo.push(p);
        }

        aleo
    }

    #[test]
    fn test_marlin_posw_bls12_377() {
        let powers = 19usize;
        let batch = 1usize << 16;
        let parameters = Phase1Parameters::<ZexeBls12_377>::new(ProvingSystem::Marlin, powers, batch);
        let expected_response_length = parameters.get_length(UseCompression::No);

        // Get a non-mutable copy of the initial accumulator state.
        let (input, _) = generate_input(&parameters, UseCompression::No, CheckForCorrectness::No);

        let mut output = vec![0; expected_response_length];

        // Construct our keypair using the RNG we created above
        let current_accumulator_hash = blank_hash();
        let mut rng = thread_rng();
        let (_, privkey) =
            Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

        Phase1::computation(
            &input,
            &mut output,
            UseCompression::No,
            UseCompression::No,
            CheckForCorrectness::No,
            &privkey,
            &parameters,
        )
        .unwrap();

        let deserialized =
            Phase1::deserialize(&output, UseCompression::No, CheckForCorrectness::No, &parameters).unwrap();
        let tau_powers_g1 = convert_vec_from_zexe_to_snarkos::<ZexeG1Affine, G1Affine>(&deserialized.tau_powers_g1);
        let tau_powers_g2 = convert_vec_from_zexe_to_snarkos::<ZexeG2Affine, G2Affine>(&deserialized.tau_powers_g2);
        let alpha_tau_powers_g1 =
            convert_vec_from_zexe_to_snarkos::<ZexeG1Affine, G1Affine>(&deserialized.alpha_tau_powers_g1);

        let h = tau_powers_g2[0].clone();
        let beta_h = tau_powers_g2[1].clone();
        let universal_params = UniversalParams::<Bls12_377> {
            powers_of_g: tau_powers_g1,
            powers_of_gamma_g: alpha_tau_powers_g1,
            h: h.clone(),
            beta_h: beta_h.clone(),
            prepared_neg_powers_of_h: Some(tau_powers_g2[2..].iter().map(|p| p.prepare()).collect::<Vec<_>>()),
            prepared_h: h.prepare(),
            prepared_beta_h: beta_h.prepare(),
        };

        let posw = PoswMarlin::index(universal_params).unwrap();

        // super low difficulty so we find a solution immediately
        let difficulty_target = 0xFFFF_FFFF_FFFF_FFFF_u64;

        let transaction_ids = vec![vec![1u8; 32]; 8];
        let (_, pedersen_merkle_root, subroots) = txids_to_roots(&transaction_ids);

        // generate the proof
        let (nonce, proof) = posw
            .mine(&subroots, difficulty_target, &mut rand::thread_rng(), std::u32::MAX)
            .unwrap();

        assert_eq!(proof.len(), 972); // NOTE: Marlin proofs use compressed serialization

        let proof = <Marlin<Bls12_377> as SNARK>::Proof::read(&proof[..]).unwrap();
        posw.verify(nonce, &proof, &pedersen_merkle_root).unwrap();
    }
}
