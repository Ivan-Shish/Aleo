use super::*;

impl<'a, E: PairingEngine + Sync> Phase1<'a, E> {
    ///
    /// Phase 1 - Verification
    ///
    /// Verifies a transformation of the `Accumulator` with the `PublicKey`,
    /// given a 64-byte transcript `digest`.
    ///
    /// Verifies that the accumulator was transformed correctly
    /// given the `PublicKey` and the so-far hash of the accumulator.
    ///
    #[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
    pub fn verification(
        input: &[u8],
        output: &[u8],
        key: &PublicKey<E>,
        digest: &[u8],
        compressed_input: UseCompression,
        compressed_output: UseCompression,
        check_input_for_correctness: CheckForCorrectness,
        check_output_for_correctness: CheckForCorrectness,
        parameters: &'a Phase1Parameters<E>,
    ) -> Result<()> {
        let span = info_span!("phase1-verification");
        let _ = span.enter();

        info!("starting...");

        // Ensure the key ratios are correctly produced
        let [tau_g2_s, alpha_g2_s, beta_g2_s] = compute_g2_s_key(&key, &digest)?;
        // put in tuple form for convenience
        let tau_g2_check = &(tau_g2_s, key.tau_g2);
        let alpha_g2_check = &(alpha_g2_s, key.alpha_g2);
        let beta_g2_check = &(beta_g2_s, key.beta_g2);
        // Check the proofs-of-knowledge for tau/alpha/beta
        let check_ratios = &[
            (key.tau_g1, tau_g2_check, "Tau G1<>G2"),
            (key.alpha_g1, alpha_g2_check, "Alpha G1<>G2"),
            (key.beta_g1, beta_g2_check, "Beta G1<>G2"),
        ];
        for (a, b, err) in check_ratios {
            check_same_ratio::<E>(a, b, err)?;
        }
        debug!("key ratios were correctly produced");

        // Split the buffers
        let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, in_beta_g2) = split(input, parameters, compressed_input);
        let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) = split(output, parameters, compressed_output);

        // Ensure that the initial conditions are correctly formed (first 2 elements)
        // We allocate a G1 vector of length 2 and re-use it for our G1 elements.
        // We keep the values of the Tau G1/G2 telements for later use.
        let (g1_check, g2_check) = {
            let mut before_g1 =
                read_initial_elements::<E::G1Affine>(in_tau_g1, compressed_input, check_input_for_correctness)?;
            let mut after_g1 =
                read_initial_elements::<E::G1Affine>(tau_g1, compressed_output, check_output_for_correctness)?;
            if after_g1[0] != E::G1Affine::prime_subgroup_generator() {
                return Err(VerificationError::InvalidGenerator(ElementType::TauG1).into());
            }
            let before_g2 =
                read_initial_elements::<E::G2Affine>(in_tau_g2, compressed_input, check_input_for_correctness)?;
            let after_g2 =
                read_initial_elements::<E::G2Affine>(tau_g2, compressed_output, check_output_for_correctness)?;
            if after_g2[0] != E::G2Affine::prime_subgroup_generator() {
                return Err(VerificationError::InvalidGenerator(ElementType::TauG2).into());
            }

            let g1_check = (after_g1[0], after_g1[1]);
            let g2_check = (after_g2[0], after_g2[1]);

            // Check TauG1 -> TauG2
            check_same_ratio::<E>(
                &(before_g1[1], after_g1[1]),
                tau_g2_check,
                "Before-After: Tau [1] G1<>G2",
            )?;
            check_same_ratio::<E>(
                &key.tau_g1,
                &(before_g2[1], after_g2[1]),
                "Before-After: Tau [1] G1<>G2",
            )?;
            let checks = match parameters.proving_system {
                ProvingSystem::Groth16 => vec![
                    (in_alpha_g1, alpha_g1, alpha_g2_check),
                    (in_beta_g1, beta_g1, beta_g2_check),
                ],
                ProvingSystem::Marlin => vec![(in_alpha_g1, alpha_g1, alpha_g2_check)],
            };
            for (before, after, check) in &checks {
                before.read_batch_preallocated(&mut before_g1, compressed_input, check_input_for_correctness)?;
                after.read_batch_preallocated(&mut after_g1, compressed_output, check_output_for_correctness)?;
                check_same_ratio::<E>(&(before_g1[0], after_g1[0]), check, "Before-After: Alpha[0] G1<>G2")?;
            }

            if parameters.proving_system == ProvingSystem::Groth16 {
                let before_beta_g2 =
                    (&*in_beta_g2).read_element::<E::G2Affine>(compressed_input, check_input_for_correctness)?;
                let after_beta_g2 =
                    (&*beta_g2).read_element::<E::G2Affine>(compressed_output, check_output_for_correctness)?;
                check_same_ratio::<E>(
                    &(before_g1[0], after_g1[0]),
                    &(before_beta_g2, after_beta_g2),
                    "Before-After: Other[0] G1<>G2",
                )?;
            }

            (g1_check, g2_check)
        };

        debug!("initial elements were computed correctly");

        // preallocate 2 vectors per batch
        // Ensure that the pairs are created correctly (we do this in chunks!)
        // load `batch_size` chunks on each iteration and perform the transformation
        iter_chunk(&parameters, |start, end| {
            debug!("verifying chunk from {} to {}", start, end);
            let span = info_span!("batch", start, end);
            let _ = span.enter();

            match parameters.proving_system {
                ProvingSystem::Groth16 => {
                    rayon::scope(|t| {
                        let _ = span.enter();

                        t.spawn(|_| {
                            let _ = span.enter();

                            let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];
                            check_power_ratios::<E>(
                                (tau_g1, compressed_output, check_output_for_correctness),
                                (start, end),
                                &mut g1,
                                &g2_check,
                            )
                            .expect("could not check ratios for Tau G1");

                            trace!("tau g1 verification successful");
                        });

                        if start < parameters.powers_length {
                            // if the `end` would be out of bounds, then just process until
                            // the end (this is necessary in case the last batch would try to
                            // process more elements than available)
                            let end = if start + parameters.batch_size > parameters.powers_length {
                                parameters.powers_length
                            } else {
                                end
                            };

                            rayon::scope(|t| {
                                let _ = span.enter();

                                t.spawn(|_| {
                                    let _ = span.enter();

                                    let mut g2 = vec![E::G2Affine::zero(); parameters.batch_size];
                                    check_power_ratios_g2::<E>(
                                        (tau_g2, compressed_output, check_output_for_correctness),
                                        (start, end),
                                        &mut g2,
                                        &g1_check,
                                    )
                                    .expect("could not check ratios for tau_g2");

                                    trace!("tau_g2 verification successful");
                                });

                                t.spawn(|_| {
                                    let _ = span.enter();

                                    let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];
                                    check_power_ratios::<E>(
                                        (alpha_g1, compressed_output, check_output_for_correctness),
                                        (start, end),
                                        &mut g1,
                                        &g2_check,
                                    )
                                    .expect("could not check ratios for alpha_g1");

                                    trace!("alpha_g1 verification successful");
                                });

                                t.spawn(|_| {
                                    let _ = span.enter();

                                    let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];
                                    check_power_ratios::<E>(
                                        (beta_g1, compressed_output, check_output_for_correctness),
                                        (start, end),
                                        &mut g1,
                                        &g2_check,
                                    )
                                    .expect("could not check ratios for beta_g1");

                                    trace!("beta_g1 verification successful");
                                });
                            });
                        }
                    });
                }
                ProvingSystem::Marlin => {
                    rayon::scope(|t| {
                        let _ = span.enter();

                        t.spawn(|_| {
                            let _ = span.enter();

                            let mut g1 = vec![E::G1Affine::zero(); parameters.batch_size];
                            check_power_ratios::<E>(
                                (tau_g1, compressed_output, check_output_for_correctness),
                                (start, end),
                                &mut g1,
                                &g2_check,
                            )
                            .expect("could not check ratios for Tau G1");

                            trace!("tau g1 verification successful");
                        });

                        //this is the first batch, check alpha g1. batch size is guaranteed to be of size >= 3
                        if start == 0 {
                            let num_alpha_powers = 3;
                            let mut g1 = vec![E::G1Affine::zero(); num_alpha_powers];
                            check_power_ratios::<E>(
                                (alpha_g1, compressed_output, check_output_for_correctness),
                                (0, num_alpha_powers),
                                &mut g1,
                                &g2_check,
                            )
                            .expect("could not check ratios for alpha_g1");

                            trace!("alpha_g1 verification successful");

                            let mut g2 = vec![E::G2Affine::zero(); 3];
                            check_power_ratios_g2::<E>(
                                (tau_g2, compressed_output, check_output_for_correctness),
                                (0, 2),
                                &mut g2,
                                &g1_check,
                            )
                            .expect("could not check ratios for tau_g2");

                            trace!("tau_g2 verification successful");
                        }
                        let powers_of_two_in_range = (0..parameters.size)
                            .map(|i| (i, parameters.powers_length as u64 - 1 - (1 << i)))
                            .map(|(i, p)| (i, p as usize))
                            .filter(|(_, p)| start <= *p && *p < end)
                            .collect::<Vec<_>>();

                        for (i, p) in powers_of_two_in_range.into_iter() {
                            let g1_size = buffer_size::<E::G1Affine>(compressed_output);
                            let g1 = (&tau_g1[p * g1_size..(p + 1) * g1_size])
                                .read_element(compressed_output, check_output_for_correctness)
                                .expect("should have read g1 element");
                            let g2_size = buffer_size::<E::G2Affine>(compressed_output);
                            let g2 = (&tau_g2[(2 + i) * g2_size..(2 + i + 1) * g2_size])
                                .read_element(compressed_output, check_output_for_correctness)
                                .expect("should have read g2 element");
                            check_same_ratio::<E>(
                                &(g1, E::G1Affine::prime_subgroup_generator()),
                                &(E::G2Affine::prime_subgroup_generator(), g2),
                                "G1<>G2",
                            )
                            .expect("should have checked same ratio");
                        }
                    });
                }
            }

            debug!("chunk verification successful");

            Ok(())
        })?;

        info!("phase1-verification complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::testing::{generate_input, generate_output};
    use setup_utils::calculate_hash;

    use zexe_algebra::{Bls12_377, BW6_761};

    use rand::thread_rng;

    fn curve_verification_test<E: PairingEngine>(
        powers: usize,
        batch: usize,
        compressed_input: UseCompression,
        compressed_output: UseCompression,
    ) {
        for proving_system in &[ProvingSystem::Marlin] {
            let parameters = Phase1Parameters::<E>::new(*proving_system, powers, batch);

            // allocate the input/output vectors
            let (input, _) = generate_input(&parameters, compressed_input, CheckForCorrectness::No);
            let mut output = generate_output(&parameters, compressed_output);

            // Construct our keypair
            let current_accumulator_hash = blank_hash();
            let mut rng = thread_rng();
            let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                .expect("could not generate keypair");

            // transform the accumulator
            Phase1::computation(
                &input,
                &mut output,
                compressed_input,
                compressed_output,
                CheckForCorrectness::No,
                &privkey,
                &parameters,
            )
            .unwrap();
            // ensure that the key is not available to the verifier
            drop(privkey);

            let res = Phase1::verification(
                &input,
                &output,
                &pubkey,
                &current_accumulator_hash,
                compressed_input,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Yes,
                &parameters,
            );
            assert!(res.is_ok());

            // subsequent participants must use the hash of the accumulator they received
            let current_accumulator_hash = calculate_hash(&output);

            let (pubkey, privkey) = Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref())
                .expect("could not generate keypair");

            // generate a new output vector for the 2nd participant's contribution
            let mut output_2 = generate_output(&parameters, compressed_output);
            // we use the first output as input
            Phase1::computation(
                &output,
                &mut output_2,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                &privkey,
                &parameters,
            )
            .unwrap();
            // ensure that the key is not available to the verifier
            drop(privkey);

            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &current_accumulator_hash,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Yes,
                &parameters,
            );
            assert!(res.is_ok());

            // verification will fail if the old hash is used
            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &blank_hash(),
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Yes,
                &parameters,
            );
            assert!(res.is_err());

            // verification will fail if even 1 byte is modified
            output_2[100] = 0;
            let res = Phase1::verification(
                &output,
                &output_2,
                &pubkey,
                &current_accumulator_hash,
                compressed_output,
                compressed_output,
                CheckForCorrectness::No,
                CheckForCorrectness::Yes,
                &parameters,
            );
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_verification_bls12_377() {
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::Yes, UseCompression::Yes);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::No, UseCompression::No);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::Yes, UseCompression::No);
        curve_verification_test::<Bls12_377>(4, 3, UseCompression::No, UseCompression::Yes);
    }

    #[test]
    fn test_verification_bw6_761() {
        curve_verification_test::<BW6_761>(4, 3, UseCompression::Yes, UseCompression::Yes);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::No, UseCompression::No);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::Yes, UseCompression::No);
        curve_verification_test::<BW6_761>(4, 3, UseCompression::No, UseCompression::Yes);
    }
}
