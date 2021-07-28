use crate::errors::VerifierError;

use snarkvm_algorithms::SignatureScheme;
use snarkvm_dpc::{testnet2::parameters::Testnet2Parameters, Address, Parameters, ViewKey};
use snarkvm_utilities::{FromBytes, ToBytes};

use rand::thread_rng;
use std::{fmt, str::FromStr};
use tracing::trace;

/// The header used for authenticating requests sent to the coordinator
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthenticationHeader {
    pub auth_type: String,
    pub address: String,
    pub signature: String,
}

impl AuthenticationHeader {
    pub fn new(auth_type: String, address: String, signature: String) -> Self {
        Self {
            auth_type,
            address,
            signature,
        }
    }
}

/// The authentication format in the header
impl fmt::Display for AuthenticationHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}:{}", self.auth_type, self.address, self.signature)
    }
}

pub struct AleoAuthentication {}

impl AleoAuthentication {
    /// Generate the authentication header with the request method, request path, and view key.
    /// Returns the authorization header "Aleo <address>:<signature>"
    pub fn authenticate(
        view_key: &ViewKey<Testnet2Parameters>,
        method: &str,
        path: &str,
    ) -> Result<AuthenticationHeader, VerifierError> {
        // Derive the Aleo address used to verify the signature.
        let address = Address::from_view_key(&view_key)?;

        // Form the message that is signed
        let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());

        trace!(
            "Request authentication - (message: {}) (address: {})",
            message,
            address.to_string()
        );

        // Construct the authentication signature.
        let signature = Self::sign(&view_key, message)?;

        // Construct the authentication header.
        Ok(AuthenticationHeader::new(
            "Aleo".to_string(),
            address.to_string(),
            signature,
        ))
    }

    ///
    /// Returns a signature created by signing a message with an Aleo view key. Otherwise,
    /// returns a `VerifierError`.
    ///
    pub fn sign(view_key: &ViewKey<Testnet2Parameters>, message: String) -> Result<String, VerifierError> {
        let rng = &mut thread_rng();

        trace!("Signing message - (message: {})", message);

        // Construct the authentication signature.
        let signature = hex::encode(view_key.sign(&message.into_bytes(), rng)?.to_bytes_le()?);

        // Construct the authentication header.
        Ok(signature)
    }

    ///
    /// Returns `true` if the signature verifies for a given address and message.
    ///
    pub fn verify(address: &str, signature: &str, message: String) -> Result<bool, VerifierError> {
        let aleo_address = Address::<Testnet2Parameters>::from_str(&address)?;
        let view_key_signature: <<Testnet2Parameters as Parameters>::AccountSignatureScheme as SignatureScheme>::Signature = FromBytes::from_bytes_le(&hex::decode(signature)?)?;

        // Check that the message verifies
        Ok(aleo_address.verify_signature(&message.to_string().into_bytes(), &view_key_signature)?)
    }

    /// Verify a request is authenticated by
    /// verifying the signature using the request method, path, and authorization header.
    pub fn verify_auth(header: &AuthenticationHeader, method: String, path: String) -> Result<bool, VerifierError> {
        // Check that the authorization header type is "aleo"
        if header.auth_type.to_lowercase() != "aleo" {
            return Ok(false);
        }

        // Extract the Aleo address and signature from the header.
        let address = &header.address;
        let signature = &header.signature;

        // Construct the message that is signed
        let message = format!("{} {}", method.to_lowercase(), path.to_lowercase());

        trace!("Authentication for address {} message is: {:?}", address, message);

        AleoAuthentication::verify(address, signature, message)
    }
}

#[cfg(test)]
mod authentication_tests {
    use super::*;

    // Example API request path
    const PATH: &str = "/v1/queue/verifier/join";

    // Example view key.
    const TEST_VIEW_KEY: &str = "AViewKey1cWNDyYMjc9p78PnCderRx37b9pJr4myQqmmPeCfeiLf3";

    #[test]
    fn test_request_authentication() {
        let view_key = ViewKey::from_str(&TEST_VIEW_KEY).unwrap();

        // Mock request parameters
        let method = "Get";
        let path = PATH;

        println!("Generating Authorization header.");

        let auth_header = AleoAuthentication::authenticate(&view_key, &method.to_string(), &path).unwrap();

        println!("Verifying request authentication");
        assert!(AleoAuthentication::verify_auth(&auth_header, method.to_string(), path.to_string()).unwrap());
    }

    #[test]
    fn test_failed_request_authentication() {
        let view_key = ViewKey::from_str(&TEST_VIEW_KEY).unwrap();

        // Create mock request parameters
        let method = "Get";
        let path = PATH;

        // Generate authorization header for the wrong method

        let incorrect_method = "Post";

        let auth_header = AleoAuthentication::authenticate(&view_key, &incorrect_method.to_string(), &path).unwrap();

        // Check that the request auth does not verify
        assert!(!AleoAuthentication::verify_auth(&auth_header, method.to_string(), path.to_string()).unwrap());
    }

    #[test]
    fn test_request_authentication_incorrect_type() {
        // Create mock request parameters
        let method = "Get";
        let path = PATH;

        // Generate and invalid authorization header

        let invalid_auth_header = AuthenticationHeader::new("TEST".to_string(), "TEST".to_string(), "TEST".to_string());

        // Check that the request auth does not verify
        assert!(!AleoAuthentication::verify_auth(&invalid_auth_header, method.to_string(), path.to_string()).unwrap());
    }
}
