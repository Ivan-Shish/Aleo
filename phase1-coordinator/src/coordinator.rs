use crate::{
    environment::{StorageType, BASE_URL},
    objects::{Chunk, Contribution, Round},
    storage::{Key, Storage, Value},
};

use std::{
    fmt,
    io,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::{error, info};
use url::Url;

#[derive(Debug)]
pub enum CoordinatorError {
    AlreadyContributed,
    FailedToUpdateChunk,
    InvalidNumberOfChunks,
    InvalidUrl,
    // #[error("IoError: {0}")]
    // IoError(#[from] io::Error),
    Launch(rocket::error::LaunchError),
    LockAlreadyAcquired,
    MissingChunk,
    MissingContributionChunk,
    MissingVerifierIds,
    NoContributions,
    NoRound,
    PreviousRoundUnfinished,
    RoundDoesNotExist,
    StorageFailed,
    UnauthorizedChunkContributor,
    UnauthorizedRoundContributor,
    Url(url::ParseError),
}

impl From<url::ParseError> for CoordinatorError {
    fn from(error: url::ParseError) -> Self {
        CoordinatorError::Url(error)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        Self::msg(error.to_string())
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct Coordinator {
    storage: Arc<RwLock<StorageType>>,
}

impl Coordinator {
    /// Creates a new instance of the `Coordinator`.
    #[inline]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(StorageType::load())),
        }
    }

    // TODO (howardwu): Ensure verification is done and continuity is enforced.
    /// Takes the previous round, checks it was fully verified, and constructs a new round.
    #[inline]
    pub fn next_round(
        &mut self,
        contributor_ids: &Vec<String>,
        verifier_ids: &Vec<String>,
        chunk_verifier_ids: &Vec<String>,
        chunk_verified_base_url: &Vec<&str>,
    ) -> Result<(), CoordinatorError> {
        info!("Starting a new round...");

        // Fetch the current height of the ceremony.
        let height = self.get_round_height()?;
        // Check that the previous round was fully completed and verified.
        if let Ok(round) = self.get_round(height) {
            if round.is_complete() {
                return Err(CoordinatorError::PreviousRoundUnfinished);
            }
        }

        // Construct the new round.
        let round = Round::new(
            0u64, /* version */
            height + 1,
            contributor_ids,
            verifier_ids,
            chunk_verifier_ids,
            chunk_verified_base_url,
        )?;

        // Add the new round.
        self.storage_mut()?.insert(Key::Round(height + 1), Value::Round(round));
        self.storage_mut()?
            .insert(Key::RoundHeight, Value::RoundHeight(height + 1));
        Ok(())
    }

    #[inline]
    pub fn get_latest_round(&self) -> Result<Round, CoordinatorError> {
        self.get_round(self.get_round_height()?)
    }

    #[inline]
    pub fn get_round(&self, height: u64) -> Result<Round, CoordinatorError> {
        // Load the round from storage.
        match self.storage()?.get(&Key::Round(height)) {
            Some(Value::Round(round)) => Ok(round.clone()),
            _ => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    #[inline]
    pub fn get_round_height(&self) -> Result<u64, CoordinatorError> {
        // Acquire the storage read lock.
        match self.storage()?.get(&Key::RoundHeight) {
            Some(Value::RoundHeight(round)) => Ok(*round),
            // This is the first round of the ceremony.
            _ => Ok(0),
        }
    }

    // #[inline]
    // pub fn get_current_round_mut(&self) -> Result<&mut Round, CoordinatorError> {
    //     // Acquire the storage read lock.
    //     let mut storage = match self.storage.write() {
    //         Ok(storage) => storage,
    //         _ => return Err(CoordinatorError::NoRound),
    //     };
    //     match storage.get_mut(&Key::CurrentRound) {
    //         Some(Value::CurrentRound(mut round)) => Ok(&mut round.clone()),
    //         _ => Err(CoordinatorError::NoRound),
    //     }
    //     // match serde_json::from_str(round_value) {
    //     //     Ok(round) => Some(round),
    //     //     _ => None,
    //     // }
    //     // let result: Round = serde_json::from_str(include_str!("./testing/ceremony.json"))?;
    // }

    #[inline]
    pub fn lock_chunk(&self, chunk_id: u64, participant_id: String) -> Result<(), CoordinatorError> {
        // Check that we are currently running a round.
        let height = self.get_round_height()?;
        let mut current_round = self.get_round(height)?;

        if !current_round.is_authorized_contributor(participant_id.clone()) {
            error!("Not authorized for /chunks/{}/lock", chunk_id);
            return Err(CoordinatorError::UnauthorizedChunkContributor);
        }

        // Check that the participant does not currently hold a lock to any chunk.
        if current_round
            .get_chunks()
            .iter()
            .filter(|chunk| chunk.is_locked_by(&participant_id))
            .next()
            .is_some()
        {
            error!("{} already holds the lock on chunk {}", participant_id, chunk_id);
            return Err(CoordinatorError::LockAlreadyAcquired);
        }

        // Fetch the chunk the participant intends to contribute to.
        let mut chunk = match current_round.get_chunk(chunk_id) {
            Some(chunk) => chunk.clone(),
            None => return Err(CoordinatorError::MissingChunk),
        };

        // Attempt to acquire the lock for the given participant ID.
        chunk.acquire_lock(&participant_id)?;

        // Attempt to update the current round chunk.
        if !current_round.set_chunk(chunk_id, &chunk) {
            return Err(CoordinatorError::FailedToUpdateChunk);
        }

        match self
            .storage_mut()?
            .insert(Key::Round(height), Value::Round(current_round))
        {
            true => Ok(()),
            false => Err(CoordinatorError::FailedToUpdateChunk),
        }
    }

    //
    // #[inline]
    // pub fn contribute_chunk(&self, chunk_id: u64, participant_id: String) -> Result<Url, CoordinatorError> {
    //     // Check that we are currently running a round.
    //     let mut current_round = self.get_current_round()?;
    //
    //     // Check that the participant is authorized to contribute.
    //     if !current_round.is_authorized_contributor(participant_id.clone()) {
    //         error!("Not authorized to acquire /chunks/{}/lock", chunk_id);
    //         return Err(CoordinatorError::UnauthorizedRoundContributor);
    //     }
    //
    //     // Fetch the chunk the participant intends to contribute to.
    //     let mut chunk = match current_round.get_chunk_mut(chunk_id) {
    //         Some(chunk) => chunk,
    //         None => return Err(CoordinatorError::MissingChunk),
    //     };
    //
    //     // Check that the participant acquired the lock.
    //     if !chunk.is_locked_by(&participant_id) {
    //         error!("{} should have lock on chunk {} but does not", participant_id, chunk_id);
    //         return Err(CoordinatorError::UnauthorizedChunkContributor);
    //     }
    //
    //     // Construct the URL for the chunk.
    //     let location = match Url::parse(&format!("{}/{}/contribution/{}", BASE_URL, chunk_id, chunk.version())) {
    //         Ok(round) => round,
    //         _ => {
    //             error!("Unable to parse the URL");
    //             return Err(CoordinatorError::InvalidUrl);
    //         }
    //     };
    //
    //     // let index = chunk.contributions.len() - 1;
    //
    //     // TODO (howardwu): Make a dedicated endpoint and method for checking verifiers.
    //     // match current_round.verifier_ids.contains(&participant_id) {
    //     //     true => match chunk.contributions.get_mut(index) {
    //     //         Some(contribution) => {
    //     //             contribution.verifier_id = Some(participant_id);
    //     //             contribution.verified_location = Some(location.clone());
    //     //             contribution.verified = true;
    //     //         }
    //     //         None => return Err(CoordinatorError::MissingContributionChunk),
    //     //     },
    //     //     false => ,
    //     // };
    //
    //     chunk.contributions.push(Contribution {
    //         contributor_id: Some(participant_id),
    //         contributed_location: Some(location.clone()),
    //         verifier_id: None,
    //         verified_location: None,
    //         verified: false,
    //     });
    //
    //     chunk.lock_holder = None;
    //     Ok(location)
    // }

    /// Attempts to acquire the read lock for storage.
    #[inline]
    fn storage(&self) -> Result<RwLockReadGuard<StorageType>, CoordinatorError> {
        match self.storage.read() {
            Ok(storage) => Ok(storage),
            _ => Err(CoordinatorError::StorageFailed),
        }
    }

    /// Attempts to acquire the write lock for storage.
    #[inline]
    fn storage_mut(&self) -> Result<RwLockWriteGuard<StorageType>, CoordinatorError> {
        match self.storage.write() {
            Ok(storage) => Ok(storage),
            _ => Err(CoordinatorError::StorageFailed),
        }
    }
}
