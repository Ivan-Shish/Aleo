use crate::{storage::InMemory, Storage};

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

pub type StorageType = InMemory;

pub(crate) const BASE_URL: &str = "http://167.71.156.62:8080";

pub enum Environment {
    Test,
    Development,
    Production,
}

impl Environment {
    pub fn number_of_chunks(&self) -> u64 {
        match self {
            Environment::Test => 10,
            Environment::Development => 5,
            Environment::Production => 5,
        }
    }
}

// lazy_static! {
//     ///
//     pub(crate) static ref STORAGE: StorageType = StorageType::new();
//     ///
//     pub(crate) static ref BASE_URL: &str = Lazy::new(|| "http://167.71.156.62:8080");
// }
//
// fn do_a_call() {
//     STORAGE.lock().unwrap().push(1);
// }
