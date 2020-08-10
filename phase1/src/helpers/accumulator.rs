//! Accumulator which operates on batches of data

use crate::{helpers::buffers::*, Phase1Parameters, PublicKey};
use setup_utils::{BatchDeserializer, BatchSerializer, Deserializer, Serializer, *};

use zexe_algebra::{AffineCurve, PairingEngine};

#[allow(type_alias_bounds)]
type AccumulatorElements<E: PairingEngine> = (
    Vec<E::G1Affine>,
    Vec<E::G2Affine>,
    Vec<E::G1Affine>,
    Vec<E::G1Affine>,
    E::G2Affine,
);

#[allow(type_alias_bounds)]
#[allow(unused)]
type AccumulatorElementsRef<'a, E: PairingEngine> = (
    &'a [E::G1Affine],
    &'a [E::G2Affine],
    &'a [E::G1Affine],
    &'a [E::G1Affine],
    &'a E::G2Affine,
);

/// Given a public key and the accumulator's digest, it hashes each G1 element
/// along with the digest, and then hashes it to G2.
pub(crate) fn compute_g2_s_key<E: PairingEngine>(key: &PublicKey<E>, digest: &[u8]) -> Result<[E::G2Affine; 3]> {
    Ok([
        compute_g2_s::<E>(&digest, &key.tau_g1.0, &key.tau_g1.1, 0)?,
        compute_g2_s::<E>(&digest, &key.alpha_g1.0, &key.alpha_g1.1, 1)?,
        compute_g2_s::<E>(&digest, &key.beta_g1.0, &key.beta_g1.1, 2)?,
    ])
}

/// Reads a list of G1 elements from the buffer to the provided `elements` slice
/// and then checks that their powers pairs ratio matches the one from the
/// provided `check` pair
pub(crate) fn check_power_ratios<E: PairingEngine>(
    (buffer, compression): (&[u8], UseCompression),
    (start, end): (usize, usize),
    elements: &mut [E::G1Affine],
    check: &(E::G2Affine, E::G2Affine),
) -> Result<()> {
    let size = buffer_size::<E::G1Affine>(compression);
    buffer[start * size..end * size].read_batch_preallocated(&mut elements[0..end - start], compression)?;
    check_same_ratio::<E>(&power_pairs(&elements[..end - start]), check, "Power pairs")?;
    Ok(())
}

/// Reads a list of G2 elements from the buffer to the provided `elements` slice
/// and then checks that their powers pairs ratio matches the one from the
/// provided `check` pair
pub(crate) fn check_power_ratios_g2<E: PairingEngine>(
    (buffer, compression): (&[u8], UseCompression),
    (start, end): (usize, usize),
    elements: &mut [E::G2Affine],
    check: &(E::G1Affine, E::G1Affine),
) -> Result<()> {
    let size = buffer_size::<E::G2Affine>(compression);
    buffer[start * size..end * size].read_batch_preallocated(&mut elements[0..end - start], compression)?;
    check_same_ratio::<E>(check, &power_pairs(&elements[..end - start]), "Power pairs")?;
    Ok(())
}

/// Reads a chunk of 2 elements from the buffer
pub(crate) fn read_initial_elements<C: AffineCurve>(buf: &[u8], compressed: UseCompression) -> Result<Vec<C>> {
    let batch = 2;
    let size = buffer_size::<C>(compressed);
    let result = buf[0..batch * size].read_batch(compressed)?;
    if result.len() != batch {
        return Err(Error::InvalidLength {
            expected: batch,
            got: result.len(),
        });
    }
    Ok(result)
}

/// Serializes all the provided elements to the output buffer
#[allow(unused)]
pub fn serialize<E: PairingEngine>(
    elements: AccumulatorElementsRef<E>,
    output: &mut [u8],
    compressed: UseCompression,
    parameters: &Phase1Parameters<E>,
) -> Result<()> {
    let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, in_beta_g2) = elements;
    let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) = split_mut(output, parameters, compressed);

    tau_g1.write_batch(&in_tau_g1, compressed)?;
    tau_g2.write_batch(&in_tau_g2, compressed)?;
    alpha_g1.write_batch(&in_alpha_g1, compressed)?;
    beta_g1.write_batch(&in_beta_g1, compressed)?;
    beta_g2.write_element(in_beta_g2, compressed)?;

    Ok(())
}

/// warning, only use this on machines which have enough memory to load
/// the accumulator in memory
pub fn deserialize<E: PairingEngine>(
    input: &[u8],
    compressed: UseCompression,
    parameters: &Phase1Parameters<E>,
) -> Result<AccumulatorElements<E>> {
    // get an immutable reference to the input chunks
    let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, in_beta_g2) = split(&input, parameters, compressed);

    // deserialize each part of the buffer separately
    let tau_g1 = in_tau_g1.read_batch(compressed)?;
    let tau_g2 = in_tau_g2.read_batch(compressed)?;
    let alpha_g1 = in_alpha_g1.read_batch(compressed)?;
    let beta_g1 = in_beta_g1.read_batch(compressed)?;
    let beta_g2 = (&*in_beta_g2).read_element(compressed)?;

    Ok((tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2))
}

/// Reads an input buffer and a secret key **which must be destroyed after this function is executed**.
pub fn decompress<E: PairingEngine>(input: &[u8], output: &mut [u8], parameters: &Phase1Parameters<E>) -> Result<()> {
    let compressed_input = UseCompression::Yes;
    let compressed_output = UseCompression::No;
    // get an immutable reference to the compressed input chunks
    let (in_tau_g1, in_tau_g2, in_alpha_g1, in_beta_g1, mut in_beta_g2) = split(&input, parameters, compressed_input);

    // get mutable refs to the decompressed outputs
    let (tau_g1, tau_g2, alpha_g1, beta_g1, beta_g2) = split_mut(output, parameters, compressed_output);

    // decompress beta_g2 for the first chunk
    {
        // get the compressed element
        let beta_g2_el = in_beta_g2.read_element::<E::G2Affine>(compressed_input)?;
        // write it back decompressed
        beta_g2.write_element(&beta_g2_el, compressed_output)?;
    }

    // load `batch_size` chunks on each iteration and decompress them
    iter_chunk(&parameters, |start, end| {
        // decompress each element
        rayon::scope(|t| {
            t.spawn(|_| {
                decompress_buffer::<E::G1Affine>(tau_g1, in_tau_g1, (start, end))
                    .expect("could not decompress the TauG1 elements")
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
                    t.spawn(|_| {
                        decompress_buffer::<E::G2Affine>(tau_g2, in_tau_g2, (start, end))
                            .expect("could not decompress the TauG2 elements")
                    });
                    t.spawn(|_| {
                        decompress_buffer::<E::G1Affine>(alpha_g1, in_alpha_g1, (start, end))
                            .expect("could not decompress the AlphaG1 elements")
                    });
                    t.spawn(|_| {
                        decompress_buffer::<E::G1Affine>(beta_g1, in_beta_g1, (start, end))
                            .expect("could not decompress the BetaG1 elements")
                    });
                });
            }
        });

        Ok(())
    })
}

/// Takes a compressed input buffer and decompresses it
fn decompress_buffer<C: AffineCurve>(output: &mut [u8], input: &[u8], (start, end): (usize, usize)) -> Result<()> {
    let in_size = buffer_size::<C>(UseCompression::Yes);
    let out_size = buffer_size::<C>(UseCompression::No);
    // read the compressed input
    let elements = input[start * in_size..end * in_size].read_batch::<C>(UseCompression::Yes)?;
    // write it back uncompressed
    output[start * out_size..end * out_size].write_batch(&elements, UseCompression::No)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::testing::random_point_vec;

    use zexe_algebra::bls12_377::Bls12_377;

    use rand::thread_rng;

    fn decompress_buffer_curve_test<C: AffineCurve>() {
        // Generate some random points.
        let mut rng = thread_rng();
        let num_els = 10;
        let elements: Vec<C> = random_point_vec(num_els, &mut rng);
        // Write them as compressed elements.
        let len = num_els * buffer_size::<C>(UseCompression::Yes);
        let mut input = vec![0; len];
        input.write_batch(&elements, UseCompression::Yes).unwrap();

        // Allocate the decompressed buffer.
        let len = num_els * buffer_size::<C>(UseCompression::No);
        let mut out = vec![0; len];
        // Perform the decompression.
        decompress_buffer::<C>(&mut out, &input, (0, num_els)).unwrap();
        let deserialized = out.read_batch::<C>(UseCompression::No).unwrap();
        // Ensure they match.
        assert_eq!(deserialized, elements);
    }

    #[test]
    fn test_decompress_buffer() {
        decompress_buffer_curve_test::<<Bls12_377 as PairingEngine>::G1Affine>();
        decompress_buffer_curve_test::<<Bls12_377 as PairingEngine>::G2Affine>();
    }
}
