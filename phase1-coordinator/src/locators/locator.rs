use crate::environment::Environment;

pub trait Locator {
    fn round_directory(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized;

    fn round_directory_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized;

    fn round_directory_reset(environment: &Environment, round_height: u64)
    where
        Self: Sized;

    fn chunk_directory(environment: &Environment, round_height: u64, chunk_id: u64) -> String
    where
        Self: Sized;

    fn chunk_directory_exists(environment: &Environment, round_height: u64, chunk_id: u64) -> bool
    where
        Self: Sized;

    /// Returns the contribution locator for a given round, chunk ID, and contribution ID from the coordinator.
    fn contribution_locator(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> String
    where
        Self: Sized;

    fn contribution_locator_exists(
        environment: &Environment,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
    ) -> bool
    where
        Self: Sized;

    /// Returns the round transcript locator for a given round from the coordinator.
    fn round_locator(environment: &Environment, round_height: u64) -> String
    where
        Self: Sized;

    fn round_locator_exists(environment: &Environment, round_height: u64) -> bool
    where
        Self: Sized;
}
