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

pub mod objects;
pub use objects::{Participant, Round};

pub(crate) mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

#[cfg(test)]
pub mod tests;
