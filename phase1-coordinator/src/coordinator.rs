use crate::{objects::Round, storage::Storage};

use once_cell::sync::Lazy;
use std::sync::{Arc, RwLock};

// lazy_static! {
//     static ref COORDINATOR: Lazy<RwLock<Coordinator>> = Lazy::new(|| Mutex::new(COORDINATOR::new()));
// }
//
// fn do_a_call() {
//     COORDINATOR.lock().unwrap().push(1);
// }

pub struct Coordinator<S> {
    storage: Arc<RwLock<S>>,
}

impl<S: Storage> Coordinator<S> {
    fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(S::new())),
        }
    }

    fn get_current_round(&self) -> Round {
        self.storage.read()
    }
}
