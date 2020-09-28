use crate::{
    environment::Environment,
    locators::Locator,
    objects::{Chunk, Round},
    CoordinatorError,
};

use std::{collections::HashMap, path::Path};
use tracing::warn;

#[derive(Debug)]
pub struct Remote;

impl Locator for Remote {
    /// Returns the transcript directory for a given round from the coordinator.
    fn round_directory(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized,
    {
        let base_url = environment.base_url();
        match environment {
            Environment::Test(_) => format!("{}/test/round/{}", base_url, round_height),
            Environment::Development(_) => format!("{}/development/round/{}", base_url, round_height),
            Environment::Production(_) => format!("{}/round/{}", base_url, round_height),
        }
    }

    fn round_directory_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized,
    {
        let url = Self::round_directory(environment, round_height);
        unimplemented!()
    }

    fn round_directory_reset(environment: &Environment, round_height: u64) {
        // If this is a test environment, attempt to clear it for the coordinator.
        // let directory = Self::round_directory(environment, round_height);
        // let path = Path::new(&directory);
        // match environment {
        //     Environment::Test(_) => {
        //         warn!("Coordinator is clearing {:?}", &path);
        //         std::fs::remove_dir_all(&path).expect("Unable to reset round directory");
        //         warn!("Coordinator cleared {:?}", &path);
        //     }
        //     Environment::Development(_) => warn!("Coordinator is attempting to clear {:?} in development mode", &path),
        //     Environment::Production(_) => warn!("Coordinator is attempting to clear {:?} in production mode", &path),
        // }
        unimplemented!()
    }

    /// Returns the chunk transcript directory for a given round and chunk ID from the coordinator.
    fn chunk_directory(environment: &Environment, round_height: u64, chunk_id: u64) -> String
    where
        Self: Sized,
    {
        // Create the transcript directory path.
        let url = Self::round_directory(environment, round_height);

        // Create the chunk directory as `{round_directory}/chunk/{chunk_id}`.
        format!("{}/chunk/{}", url, chunk_id)
    }

    fn chunk_directory_exists(environment: &Environment, round_height: u64, chunk_id: u64) -> bool
    where
        Self: Sized,
    {
        let path = Self::chunk_directory(environment, round_height, chunk_id);
        Path::new(&path).exists()
    }

    /// Returns the contribution locator for a given round, chunk ID, and contribution ID from the coordinator.
    fn contribution_locator(environment: &Environment, round_height: u64, chunk_id: u64, contribution_id: u64) -> String
    where
        Self: Sized,
    {
        // Fetch the chunk directory path.
        let path = Self::chunk_directory(environment, round_height, chunk_id);

        // If the path does not exist, attempt to initialize the directory path.
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path).expect("unable to create the chunk directory");
        }

        // Set the contribution locator as `{chunk_directory}/contribution/{contribution_id}`.
        format!("{}/contribution/{}", path, contribution_id)
    }

    fn contribution_locator_exists(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> bool
    where
        Self: Sized,
    {
        let path = Self::contribution_locator(environment, round_height, chunk_id, contribution_id);
        Path::new(&path).exists()
    }

    /// Returns the round transcript locator for a given round from the coordinator.
    fn round_locator(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized,
    {
        // Create the transcript directory path.
        let path = Self::round_directory(environment, round_height);

        // If the path does not exist, attempt to initialize the directory path.
        // if !Path::new(&path).exists() {
        //     std::fs::create_dir_all(&path).expect("unable to create the chunk transcript directory");
        // }

        // Create the round locator located at `{round_directory}/round`.
        format!("{}/round", path)
    }

    fn round_locator_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized,
    {
        let path = Self::round_locator(environment, round_height);
        Path::new(&path).exists()
    }
}
