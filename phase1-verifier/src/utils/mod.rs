pub mod authentication;
pub use authentication::*;

pub mod logger;
pub use logger::*;

use std::{fs, path::Path};
use tracing::error;

///
/// This function writes a `file_bytes` to the `locator` path.
///
/// If a parent directory doesn't already exists, this function will
/// automatically generate one.
///
pub fn write_to_file(locator: &str, file_bytes: Vec<u8>) {
    create_parent_directory(locator);

    // Write the response file to a the response_locator_path
    if let Err(err) = fs::write(locator, file_bytes) {
        error!("Error writing file to path {} (error: {})", &locator, err);
    }
}

///
/// This function creates the `locator` path's parent directories if it
/// does not  already exists.
///
pub fn create_parent_directory(locator: &str) {
    // Create the parent directory if it doesn't already exist
    let locator_path = Path::new(&locator);
    if let Some(locator_path_parent) = locator_path.parent() {
        if let Err(err) = fs::create_dir_all(locator_path_parent) {
            error!(
                "Error initializing locator parent directory {:?} (error: {})",
                &locator_path_parent, err
            );
        }
    }
}
