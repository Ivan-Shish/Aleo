pub mod authentication;
pub use authentication::*;

pub mod logger;
pub use logger::*;

use std::{fs, path::Path};
use tracing::{error, trace};

///
/// This function writes a `file_bytes` to the `locator` path.
///
/// If a parent directory doesn't already exists, this function will
/// automatically generate one.
///
pub fn write_to_file(locator: &str, file_bytes: Vec<u8>) {
    create_parent_directory(locator);
    remove_file_if_exists(locator);

    if let Err(err) = fs::write(locator, file_bytes) {
        error!("Error writing file to path {} {}", &locator, err);
    }
}

// ///
// /// This function reads the bytes from a file at a given path.
// ///
// pub fn read_from_file(locator: &str) -> anyhow::Result<Vec<u8>> {
//     let mut buffer = Vec::new();
//     let mut file = fs::File::open(locator)?;
//     file.read_to_end(&mut buffer)?;
//
//     Ok(buffer)
// }

///
/// This function creates the `locator` path's parent directories if it
/// does not already exists.
///
pub fn create_parent_directory(locator: &str) {
    let locator_path = Path::new(&locator);
    if let Some(locator_path_parent) = locator_path.parent() {
        if let Err(err) = fs::create_dir_all(locator_path_parent) {
            error!(
                "Error initializing locator parent directory {:?} {}",
                &locator_path_parent, err
            );
        }
    }
}

///
/// This function removes a file if it exists in the filesystem.
///
pub fn remove_file_if_exists(file_path: &str) {
    if Path::new(file_path).exists() {
        trace!("Removing file {}", file_path);
        if let Err(err) = fs::remove_file(file_path) {
            error!("Error removing file {} {}", &file_path, err);
        }
    }
}
