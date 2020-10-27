/// A core structure for authentication of contributions.
pub trait Signature: Send + Sync {
    /// Returns the name of the signature scheme.
    fn name(&self) -> String;

    /// Returns `true` if the signature scheme is safe for use in production.
    fn is_secure(&self) -> bool;

    /// Signs the given message using the given secret key,
    /// and returns the signature as a string.
    fn sign(&self, secret_key: &str, message: &str) -> anyhow::Result<String>;

    /// Verifies the given signature for the given message and public key,
    /// and returns `true` if the signature is valid.
    fn verify(&self, public_key: &str, message: &str, signature: &str) -> bool;
}
