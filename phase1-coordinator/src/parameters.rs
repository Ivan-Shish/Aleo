use crate::{storage::InMemory, Storage};

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

pub type StorageType = InMemory;

pub(crate) const BASE_URL: &str = "http://167.71.156.62:8080";

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
