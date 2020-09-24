#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate serde_json;

pub mod apis;
pub mod parameters;

pub mod coordinator;
pub use coordinator::*;

mod objects;

pub mod storage;
pub use storage::Storage;
