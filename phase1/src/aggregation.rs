use super::*;

impl<'a, E: PairingEngine + Sync> Phase1<'a, E> {
    ///
    /// Phase 1: Aggregation
    ///
    /// Verifies that the accumulator was transformed correctly,
    /// given the `PublicKey` and the intermediate hash of the accumulator.
    /// This verifies a single chunk and checks only that the points
    /// are not zero and that they're in the prime order subgroup.
    ///
    pub fn aggregation(
        inputs: &[(&[u8], UseCompression)],
        (output, compressed_output): (&mut [u8], UseCompression),
        parameters: &Phase1Parameters<E>,
    ) -> Result<()> {
        let span = info_span!("phase1-aggregation");
        let _enter = span.enter();

        info!("starting...");

        // TODO (howardwu): Handle the Marlin mode case.

        for (chunk_index, (input, compressed_input)) in inputs.iter().enumerate() {
            let chunk_parameters =
                parameters.into_chunk_parameters(parameters.contribution_mode, chunk_index, parameters.chunk_size);

            let input = *input;
            let compressed_input = *compressed_input;

            let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, in_beta_g2) =
                split(input, &chunk_parameters, compressed_input);
            let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) =
                split_at_chunk_mut(output, &chunk_parameters, compressed_output);

            let start = chunk_index * chunk_parameters.chunk_size;
            let end = (chunk_index + 1) * chunk_parameters.chunk_size;

            debug!("combining chunk from {} to {}", start, end);

            let span = info_span!("batch", start, end);
            let _enter = span.enter();

            rayon::scope(|t| {
                let _enter = span.enter();

                t.spawn(|_| {
                    let _enter = span.enter();

                    let elements: Vec<E::G1Affine> = in_tau_g1
                        .read_batch(compressed_input, CheckForCorrectness::No)
                        .expect("should have read batch");
                    tau_g1
                        .write_batch(&elements, compressed_output)
                        .expect("should have written batch");

                    trace!("tau_g1 aggregation for chunk {} successful", chunk_index);
                });

                if start < chunk_parameters.powers_length {
                    rayon::scope(|t| {
                        let _enter = span.enter();

                        t.spawn(|_| {
                            let _enter = span.enter();

                            let elements: Vec<E::G2Affine> = in_tau_g2
                                .read_batch(compressed_input, CheckForCorrectness::No)
                                .expect("should have read batch");
                            tau_g2
                                .write_batch(&elements, compressed_output)
                                .expect("should have written batch");

                            trace!("tau_g2 aggregation for chunk {} successful", chunk_index);
                        });

                        t.spawn(|_| {
                            let _enter = span.enter();

                            let elements: Vec<E::G1Affine> = in_alpha_g1
                                .read_batch(compressed_input, CheckForCorrectness::No)
                                .expect("should have read batch");
                            alpha_g1
                                .write_batch(&elements, compressed_output)
                                .expect("should have written batch");

                            trace!("alpha_g1 aggregation for chunk {} successful", chunk_index);
                        });

                        t.spawn(|_| {
                            let _enter = span.enter();

                            let elements: Vec<E::G1Affine> = in_beta_g1
                                .read_batch(compressed_input, CheckForCorrectness::No)
                                .expect("should have read batch");
                            beta_g1
                                .write_batch(&elements, compressed_output)
                                .expect("should have written batch");

                            trace!("beta_g1 aggregation for chunk {} successful", chunk_index);
                        });
                    });
                }

                if chunk_index == 0 {
                    let element: E::G2Affine = (&*in_beta_g2)
                        .read_element(compressed_input, CheckForCorrectness::No)
                        .expect("should have read element");
                    beta_g2
                        .write_element(&element, compressed_output)
                        .expect("should have written element");
                    trace!("beta_g2 aggregation for chunk {} successful", chunk_index);
                }
            });

            debug!("chunk {} processing successful", chunk_index);
        }

        info!("phase1-aggregation complete");

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     use zexe_algebra::{AffineCurve, Bls12_377, BW6_761};
//
//     fn curve_aggregation_test<E: PairingEngine>(powers: usize, batch: usize, compression: UseCompression) {
//         for proving_system in &[ProvingSystem::Groth16, ProvingSystem::Marlin] {
//             let parameters = Phase1Parameters::<E>::new(*proving_system, powers, batch);
//             let expected_challenge_length = match compression {
//                 UseCompression::Yes => parameters.contribution_size - parameters.public_key_size,
//                 UseCompression::No => parameters.accumulator_size,
//             };
//
//             let mut output = vec![0; expected_challenge_length];
//             Phase1::initialization(&mut output, compression, &parameters).unwrap();
//
//             let deserialized =
//                 Phase1::deserialize(&output, compression, CheckForCorrectness::Full, &parameters).unwrap();
//
//             let g1_zero = E::G1Affine::prime_subgroup_generator();
//             let g2_zero = E::G2Affine::prime_subgroup_generator();
//
//             match parameters.proving_system {
//                 ProvingSystem::Groth16 => {
//                     assert_eq!(deserialized.tau_powers_g1, vec![g1_zero; parameters.powers_g1_length]);
//                     assert_eq!(deserialized.tau_powers_g2, vec![g2_zero; parameters.powers_length]);
//                     assert_eq!(deserialized.alpha_tau_powers_g1, vec![
//                         g1_zero;
//                         parameters.powers_length
//                     ]);
//                     assert_eq!(deserialized.beta_tau_powers_g1, vec![g1_zero; parameters.powers_length]);
//                     assert_eq!(deserialized.beta_g2, g2_zero);
//                 }
//                 ProvingSystem::Marlin => {
//                     assert_eq!(deserialized.tau_powers_g1, vec![g1_zero; parameters.powers_length]);
//                     assert_eq!(deserialized.tau_powers_g2, vec![g2_zero; parameters.size + 2]);
//                     assert_eq!(deserialized.alpha_tau_powers_g1, vec![g1_zero; 3 + 3 * parameters.size]);
//                 }
//             }
//         }
//     }
//
//     #[test]
//     fn test_aggregation_bls12_377_compressed() {
//         curve_aggregation_test::<Bls12_377>(4, 4, UseCompression::Yes);
//     }
//
//     #[test]
//     fn test_aggregation_bls12_377_uncompressed() {
//         curve_aggregation_test::<Bls12_377>(4, 4, UseCompression::No);
//     }
//
//     #[test]
//     fn test_aggregation_bw6_761_compressed() {
//         curve_aggregation_test::<BW6_761>(4, 4, UseCompression::Yes);
//     }
//
//     #[test]
//     fn test_aggregation_bw6_761_uncompressed() {
//         curve_aggregation_test::<BW6_761>(4, 4, UseCompression::No);
//     }
// }
