#[cfg(any(test, feature = "testing"))]
#[macro_use]
extern crate lazy_static;

#[macro_use]
mod macros;

pub(crate) mod commands;

pub mod coordinator;
pub use coordinator::*;

pub(crate) mod coordinator_state;
pub(crate) use coordinator_state::CoordinatorState;

pub mod environment;

#[cfg(not(test))]
pub mod logger;

pub mod objects;
pub use objects::{ContributionFileSignature, Participant, Round};

mod serialize;

pub(crate) mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[cfg(test)]
pub mod tests;
