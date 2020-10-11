#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rocket;

#[macro_use]
mod macros;

pub mod commands;
pub mod coordinator;
pub mod environment;

pub use coordinator::*;
pub use objects::{Participant, Round};
pub use storage::Storage;

mod objects;
mod storage;

#[cfg(any(test, feature = "testing"))]
pub mod testing;
