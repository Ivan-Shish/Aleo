use setup_utils::UseCompression;

use zexe_algebra::{ConstantSerializedSize, PairingEngine};

use std::marker::PhantomData;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProvingSystem {
    Groth16,
    Marlin,
}

/// The sizes of the group elements of a curve
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct CurveParameters<E> {
    /// Size of a G1 Element
    pub g1_size: usize,
    /// Size of a G2 Element
    pub g2_size: usize,
    /// Size of a compressed G1 Element
    pub g1_compressed_size: usize,
    /// Size of a compressed G2 Element
    pub g2_compressed_size: usize,
    engine_type: PhantomData<E>,
}

impl<E: PairingEngine> CurveParameters<E> {
    pub fn new() -> CurveParameters<E> {
        CurveParameters {
            g1_size: <E as PairingEngine>::G1Affine::UNCOMPRESSED_SIZE,
            g2_size: <E as PairingEngine>::G2Affine::UNCOMPRESSED_SIZE,
            g1_compressed_size: <E as PairingEngine>::G1Affine::SERIALIZED_SIZE,
            g2_compressed_size: <E as PairingEngine>::G2Affine::SERIALIZED_SIZE,
            engine_type: PhantomData,
        }
    }
}

/// The parameters used for the trusted setup ceremony
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase1Parameters<E> {
    /// The type of the curve being used
    pub curve: CurveParameters<E>,
    /// The number of Powers of Tau G1 elements which will be accumulated
    pub powers_g1_length: usize,
    /// The number of Powers of Tau Alpha/Beta/G2 elements which will be accumulated
    pub powers_length: usize,
    /// The circuit size exponent (ie length will be 2^size),
    /// depends on the computation you want to support.
    pub size: usize,
    /// The empirical batch size for the batched accumulator.
    /// This is a hyper parameter and may be different for each curve.
    pub batch_size: usize,
    /// Size of the used public key
    pub public_key_size: usize,
    /// Total size of the accumulator used for the ceremony
    pub accumulator_size: usize,
    /// Total size of the contribution
    pub contribution_size: usize,
    /// Size of the hash of the previous contribution
    pub hash_size: usize,
    /// Proving system
    pub proving_system: ProvingSystem,
}

impl<E: PairingEngine> Phase1Parameters<E> {
    /// Constructs a new ceremony parameters object from the type of provided curve
    /// Panics if given batch_size = 0
    pub fn new(proving_system: ProvingSystem, size: usize, batch_size: usize) -> Self {
        // create the curve
        let curve = CurveParameters::<E>::new();
        Self::new_with_curve(curve, proving_system, size, batch_size)
    }

    /// Constructs a new ceremony parameters object from the directly provided curve with parameters
    /// Consider using the `new` method if you want to use one of the pre-implemented curves
    pub fn new_with_curve(
        curve: CurveParameters<E>,
        proving_system: ProvingSystem,
        size: usize,
        batch_size: usize,
    ) -> Self {
        // assume we're using a 64 byte long hash function such as Blake
        let hash_size = 64;

        // 2^{size}
        let powers_length = 1 << size;
        // 2^{size+1} - 1
        let powers_g1_length = (powers_length << 1) - 1;

        let accumulator_size = match proving_system {
            ProvingSystem::Groth16 => {
                // G1 Tau powers
                powers_g1_length * curve.g1_size +
                    // G2 Tau Powers + Alpha Tau powers + Beta Tau powers
                    powers_length * (curve.g2_size + (curve.g1_size * 2)) +
                    // Beta in G2
                    curve.g2_size +
                    // Hash of the previous contribution
                    hash_size
            }
            ProvingSystem::Marlin => {
                // G1 Tau powers
                powers_length * curve.g1_size +
                    // Alpha in G1
                    3*curve.g1_size +
                    // G2 1/Tau Powers
                    (size + 2) * curve.g2_size +
                    // Hash of the previous contribution
                    hash_size
            }
        };

        let public_key_size =
           // tau, alpha, beta in g2
           3 * curve.g2_size +
           // (s1, s1*tau), (s2, s2*alpha), (s3, s3*beta) in g1
           6 * curve.g1_size;

        let contribution_size = match proving_system {
            ProvingSystem::Groth16 => {
                // G1 Tau powers (compressed)
                powers_g1_length * curve.g1_compressed_size +
                    // G2 Tau Powers + Alpha Tau powers + Beta Tau powers (compressed)
                    powers_length * (curve.g2_compressed_size + (curve.g1_compressed_size * 2)) +
                    // Beta in G2
                    curve.g2_compressed_size +
                    // Hash of the previous contribution
                    hash_size +
                    // The public key of the previous contributor
                    public_key_size
            }
            ProvingSystem::Marlin => {
                // G1 Tau powers (compressed)
                powers_length * curve.g1_compressed_size +
                    // Alpha in G1
                    3*curve.g1_compressed_size +
                    // G2 1/Tau Powers
                    (size + 2) * curve.g2_compressed_size +
                    // Hash of the previous contribution
                    hash_size +
                    // The public key of the previous contributor
                    public_key_size
            }
        };

        Self {
            curve,
            powers_g1_length,
            powers_length,
            size,
            batch_size,
            accumulator_size,
            public_key_size,
            contribution_size,
            hash_size,
            proving_system,
        }
    }

    /// Returns the length of the serialized accumulator depending on if it's compressed or not
    pub fn get_length(&self, compressed: UseCompression) -> usize {
        match compressed {
            UseCompression::Yes => self.contribution_size - self.public_key_size,
            UseCompression::No => self.accumulator_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zexe_algebra::{Bls12_377, Bls12_381, BW6_761};

    fn curve_params_test<E: PairingEngine>(g1: usize, g2: usize, g1_compressed: usize, g2_compressed: usize) {
        let p = CurveParameters::<E>::new();
        assert_eq!(p.g1_size, g1);
        assert_eq!(p.g2_size, g2);
        assert_eq!(p.g1_compressed_size, g1_compressed);
        assert_eq!(p.g2_compressed_size, g2_compressed);
    }

    #[test]
    fn test_parameter_sizes() {
        curve_params_test::<Bls12_377>(96, 192, 48, 96);
        curve_params_test::<Bls12_381>(96, 192, 48, 96);
        curve_params_test::<BW6_761>(192, 192, 96, 96);
    }
}
