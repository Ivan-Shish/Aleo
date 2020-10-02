#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate thiserror;

pub mod authentication;
pub mod errors;
pub mod logger;
pub mod verifier;
