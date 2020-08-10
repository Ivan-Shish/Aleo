use crate::Phase1Parameters;
use setup_utils::{BatchDeserializer, BatchSerializer, *};

use zexe_algebra::{AffineCurve, PairingEngine};

use itertools::{Itertools, MinMaxResult};

/// Buffer, compression
type Input<'a> = (&'a [u8], UseCompression);

/// Mutable buffer, compression
type Output<'a> = (&'a mut [u8], UseCompression);

/// Mutable slices with format [TauG1, TauG2, AlphaG1, BetaG1, BetaG2]
type SplitBufMut<'a> = (&'a mut [u8], &'a mut [u8], &'a mut [u8], &'a mut [u8], &'a mut [u8]);

/// Immutable slices with format [TauG1, TauG2, AlphaG1, BetaG1, BetaG2]
type SplitBuf<'a> = (&'a [u8], &'a [u8], &'a [u8], &'a [u8], &'a [u8]);

/// Helper function to iterate over the accumulator in chunks.
/// `action` will perform an action on the chunk
pub(crate) fn iter_chunk(
    parameters: &Phase1Parameters<impl PairingEngine>,
    mut action: impl FnMut(usize, usize) -> Result<()>,
) -> Result<()> {
    (0..parameters.powers_g1_length)
        .chunks(parameters.batch_size)
        .into_iter()
        .map(|chunk| {
            let (start, end) = match chunk.minmax() {
                MinMaxResult::MinMax(start, end) => (start, end + 1),
                MinMaxResult::OneElement(start) => (start, start + 1),
                _ => return Err(Error::InvalidChunk),
            };
            action(start, end)
        })
        .collect::<Result<_>>()
}

/// Takes a buffer, reads the group elements in it, exponentiates them to the
/// provided `powers` and maybe to the `coeff`, and then writes them back
pub(crate) fn apply_powers<C: AffineCurve>(
    (output, output_compressed): Output,
    (input, input_compressed): Input,
    (start, end): (usize, usize),
    powers: &[C::ScalarField],
    coeff: Option<&C::ScalarField>,
) -> Result<()> {
    let in_size = buffer_size::<C>(input_compressed);
    let out_size = buffer_size::<C>(output_compressed);
    // read the input
    let mut elements = &mut input[start * in_size..end * in_size].read_batch::<C>(input_compressed)?;
    // calculate the powers
    batch_exp(&mut elements, &powers[..end - start], coeff)?;
    // write back
    output[start * out_size..end * out_size].write_batch(&elements, output_compressed)?;

    Ok(())
}

/// Splits the full buffer in 5 non overlapping mutable slice.
/// Each slice corresponds to the group elements in the following order
/// [TauG1, TauG2, AlphaG1, BetaG1, BetaG2]
pub(crate) fn split_mut<'a, E: PairingEngine>(
    buf: &'a mut [u8],
    parameters: &'a Phase1Parameters<E>,
    compressed: UseCompression,
) -> SplitBufMut<'a> {
    let num_powers = parameters.powers_length;
    let num_powers_g1 = parameters.powers_g1_length;

    let g1_size = buffer_size::<E::G1Affine>(compressed);
    let g2_size = buffer_size::<E::G2Affine>(compressed);

    // Set the first 64 bytes for the hash
    let (_, others) = buf.split_at_mut(parameters.hash_size);

    let (tau_g1, others) = others.split_at_mut(g1_size * num_powers_g1);
    let (tau_g2, others) = others.split_at_mut(g2_size * num_powers);
    let (alpha_g1, others) = others.split_at_mut(g1_size * num_powers);
    let (beta_g1, beta_g2) = others.split_at_mut(g1_size * num_powers);

    // We take up to g2_size for beta_g2, as there may be other
    // elements after it at the end of the buffer.
    (tau_g1, tau_g2, alpha_g1, beta_g1, &mut beta_g2[0..g2_size])
}

/// Splits the full buffer in 5 non overlapping immutable slice.
/// Each slice corresponds to the group elements in the following order
/// [TauG1, TauG2, AlphaG1, BetaG1, BetaG2]
pub(crate) fn split<'a, E: PairingEngine>(
    buf: &'a [u8],
    parameters: &Phase1Parameters<E>,
    compressed: UseCompression,
) -> SplitBuf<'a> {
    let g1_els = parameters.powers_g1_length;
    let other = parameters.powers_length;
    let g1_size = buffer_size::<E::G1Affine>(compressed);
    let g2_size = buffer_size::<E::G2Affine>(compressed);

    let (_, others) = buf.split_at(parameters.hash_size);
    let (tau_g1, others) = others.split_at(g1_size * g1_els);
    let (tau_g2, others) = others.split_at(g2_size * other);
    let (alpha_g1, others) = others.split_at(g1_size * other);
    let (beta_g1, beta_g2) = others.split_at(g1_size * other);
    // we take up to g2_size for beta_g2, since there might be other
    // elements after it at the end of the buffer
    (tau_g1, tau_g2, alpha_g1, beta_g1, &beta_g2[0..g2_size])
}
