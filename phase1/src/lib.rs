pub mod cli_common;
pub mod helpers;

pub mod objects;
pub use objects::*;

mod computation;
mod initialization;
mod key_generation;
mod serialization;
mod verification;

use crate::helpers::raw_accumulator::{self, *};
use snark_utils::{
    blank_hash,
    check_same_ratio,
    compute_g2_s,
    generate_powers_of_tau,
    BatchDeserializer,
    BatchSerializer,
    Deserializer,
    ElementType,
    Error,
    GenericArray,
    Result,
    Serializer,
    UseCompression,
    VerificationError,
    U64,
};

use zexe_algebra::{AffineCurve, PairingEngine, ProjectiveCurve, UniformRand, Zero};

use rand::Rng;
use tracing::{debug, info, info_span, trace};

/// The `BatchedAccumulator` is an object that participants of the ceremony contribute
/// randomness to. This object contains powers of trapdoor `tau` in G1 and in G2 over
/// fixed generators, and additionally in G1 over two other generators of exponents
/// `alpha` and `beta` over those fixed generators. In other words:
///
/// * (τ, τ<sup>2</sup>, ..., τ<sup>2<sup>22</sup> - 2</sup>, α, ατ, ατ<sup>2</sup>, ..., ατ<sup>2<sup>21</sup> - 1</sup>, β, βτ, βτ<sup>2</sup>, ..., βτ<sup>2<sup>21</sup> - 1</sup>)<sub>1</sub>
/// * (β, τ, τ<sup>2</sup>, ..., τ<sup>2<sup>21</sup> - 1</sup>)<sub>2</sub>
#[derive(Debug)]
pub struct Phase1<'a, E: PairingEngine> {
    /// tau^0, tau^1, tau^2, ..., tau^{TAU_POWERS_G1_LENGTH - 1}
    pub tau_powers_g1: Vec<E::G1Affine>,
    /// tau^0, tau^1, tau^2, ..., tau^{TAU_POWERS_LENGTH - 1}
    pub tau_powers_g2: Vec<E::G2Affine>,
    /// alpha * tau^0, alpha * tau^1, alpha * tau^2, ..., alpha * tau^{TAU_POWERS_LENGTH - 1}
    pub alpha_tau_powers_g1: Vec<E::G1Affine>,
    /// beta * tau^0, beta * tau^1, beta * tau^2, ..., beta * tau^{TAU_POWERS_LENGTH - 1}
    pub beta_tau_powers_g1: Vec<E::G1Affine>,
    /// beta
    pub beta_g2: E::G2Affine,
    /// Hash chain hash
    pub hash: GenericArray<u8, U64>,
    /// The parameters used for the setup of this accumulator
    pub parameters: &'a Phase1Parameters<E>,
}

impl<'a, E: PairingEngine> PartialEq for Phase1<'a, E> {
    fn eq(&self, other: &Self) -> bool {
        self.tau_powers_g1 == other.tau_powers_g1
            && self.tau_powers_g2 == other.tau_powers_g2
            && self.alpha_tau_powers_g1 == other.alpha_tau_powers_g1
            && self.beta_tau_powers_g1 == other.beta_tau_powers_g1
            && self.beta_g2 == other.beta_g2
            && self.hash == other.hash
    }
}
