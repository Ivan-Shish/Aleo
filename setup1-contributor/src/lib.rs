#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

#[cfg(feature = "azure")]
pub mod blobstore;

pub mod cli;
pub mod commands;
pub mod errors;
pub mod objects;
mod reliability;
pub mod tasks;
pub mod utils;
