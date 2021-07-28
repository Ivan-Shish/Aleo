// use phase1::{helpers::testing::setup_verify, Phase1, Phase1Parameters, ProvingSystem};
// use phase2::{helpers::testing::TestCircuit, parameters::MPCParameters};
// use setup_utils::{CheckForCorrectness, Groth16Params, UseCompression};
//
// use zexe_algebra::{serialize::CanonicalSerialize, Bls12_377, PairingEngine as ZexePairingEngine, BW6_761};
// use zexe_groth16::Parameters;
//
// use snarkos_algorithms::snark::groth16::{
//     create_random_proof,
//     prepare_verifying_key,
//     verify_proof,
//     Parameters as AleoGroth16Params,
// };
// use snarkos_curves::{bls12_377::Bls12_377 as AleoBls12_377, bw6_761::BW6_761 as AleoBW6};
// use snarkos_models::{
//     curves::PairingEngine as AleoPairingEngine,
//     gadgets::r1cs::{ConstraintCounter, ConstraintSynthesizer},
// };
// use snarkos_utilities::serialize::CanonicalDeserialize;
//
// use rand::{thread_rng, Rng};
//
// fn generate_mpc_parameters<Aleo: AleoPairingEngine, Zexe: ZexePairingEngine, C, R: Rng + CryptoRng>(
//     proving_system: ProvingSystem,
//     c: C,
//     rng: &mut R,
// ) -> MPCParameters<Zexe>
// where
//     C: Clone + ConstraintSynthesizer<Aleo::Fr>,
// {
//     let powers = 6; // Powers of tau
//     let batch = 4;
//     let params = Phase1Parameters::<Zexe>::new_full(proving_system, powers, batch);
//     let compressed = UseCompression::Yes;
//     // Make 1 power of tau contribution (assume powers of tau gets calculated properly).
//     let (_, output, _, _) = setup_verify(compressed, CheckForCorrectness::Full, compressed, &params);
//     let accumulator = Phase1::deserialize(&output, compressed, CheckForCorrectness::Full, &params).unwrap();
//
//     // Prepare only the first 32 powers (for whatever reason).
//     let groth_params = Groth16Params::<Zexe>::new(
//         32,
//         accumulator.tau_powers_g1,
//         accumulator.tau_powers_g2,
//         accumulator.alpha_tau_powers_g1,
//         accumulator.beta_tau_powers_g1,
//         accumulator.beta_g2,
//     )
//     .unwrap();
//     // Write the transcript to a file.
//     let mut writer = vec![];
//     groth_params.write(&mut writer, compressed).unwrap();
//
//     // perform the MPC on only the amount of constraints required for the circuit
//     let mut counter = ConstraintCounter::new();
//     c.clone().generate_constraints(&mut counter).unwrap();
//     let phase2_size = std::cmp::max(counter.num_constraints, counter.num_aux + counter.num_inputs + 1);
//
//     let mut mpc = MPCParameters::<Zexe>::new_from_buffer::<Aleo, _>(
//         c,
//         writer.as_mut(),
//         compressed,
//         CheckForCorrectness::Full,
//         32,
//         phase2_size,
//     )
//     .unwrap();
//
//     let before = mpc.clone();
//     // It is _not_ safe to use it yet, there must be 1 contribution.
//     mpc.contribute(rng).unwrap();
//
//     before.verify(&mpc).unwrap();
//
//     mpc
// }
//
// fn test_groth16_curve<Aleo: AleoPairingEngine, Zexe: ZexePairingEngine>() {
//     let rng = &mut thread_rng();
//     // Generate the parameters.
//     let params: Parameters<Zexe> = {
//         let c = TestCircuit::<Aleo>(None);
//         let setup = generate_mpc_parameters::<Aleo, Zexe, _, _>(ProvingSystem::Groth16, c, rng);
//         setup.get_params().clone()
//     };
//
//     // Convert the Zexe parameters to Aleo parameters.
//     let mut v = Vec::new();
//     params.serialize(&mut v).unwrap();
//     let params = AleoGroth16Params::<Aleo>::deserialize(&mut &v[..]).unwrap();
//
//     // Prepare the verification key (for proof verification)
//     let pvk = prepare_verifying_key(&params.vk);
//
//     // Create a proof with these parameters.
//     let proof = {
//         let c = TestCircuit::<Aleo>(Some(Aleo::Fr::from(5)));
//         create_random_proof(c, &params, rng).unwrap()
//     };
//
//     let res = verify_proof(&pvk, &proof, &[Aleo::Fr::from(25u8)]);
//     assert!(res.is_ok());
// }
//
// #[test]
// fn test_groth16_bls12_377() {
//     test_groth16_curve::<AleoBls12_377, Bls12_377>()
// }
//
// #[test]
// fn test_groth16_bw6() {
//     test_groth16_curve::<AleoBW6, BW6_761>()
// }
