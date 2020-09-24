use crate::{
    objects::{Ceremony, Contribution, Round},
    parameters::{StorageType, BASE_URL},
    storage::{Key, Storage, Value},
};

use std::sync::{Arc, RwLock};
use url::Url;

pub enum CoordinatorError {
    FailedToAcquireLock,
    FailedToUpdateChunk,
    InvalidUrl,
    LockAlreadyAcquired,
    MissingChunk,
    MissingContributionChunk,
    NoCeremony,
    NoRound,
    StorageFailed,
    UnauthorizedRoundContributor,
    UnauthorizedChunkContributor,
}

pub struct Coordinator {
    storage: Arc<RwLock<StorageType>>,
}

impl Coordinator {
    /// Creates a new instance of the `Coordinator`.
    #[inline]
    pub fn new() -> Self {
        let storage = StorageType::load();

        // // Resume the current round if it was previously running. Otherwise set to `None`.
        // let current_round = match storage.get(&Key::CurrentRound) {
        //     Some(Value::CurrentRound(round)) => Some(&*round.clone()),
        //     _ => None,
        // };

        let storage = Arc::new(RwLock::new(storage));
        Self { storage }
    }

    #[inline]
    pub fn get_ceremony(&self) -> Result<Ceremony, CoordinatorError> {
        // Acquire the storage read lock.
        let storage = match self.storage.read() {
            Ok(storage) => storage,
            _ => return Err(CoordinatorError::NoCeremony),
        };
        // Load the ceremony from storage.
        match storage.get(&Key::Ceremony) {
            Some(Value::Ceremony(ceremony)) => Ok(ceremony.clone()),
            _ => Err(CoordinatorError::NoCeremony),
        }
    }

    #[inline]
    pub fn get_current_round(&self) -> Result<Round, CoordinatorError> {
        // Acquire the storage read lock.
        let storage = match self.storage.read() {
            Ok(storage) => storage,
            _ => {
                error!("No round is currently running");
                return Err(CoordinatorError::NoRound);
            }
        };
        match storage.get(&Key::CurrentRound) {
            Some(Value::CurrentRound(round)) => Ok(round.clone()),
            _ => Err(CoordinatorError::NoRound),
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
        let mut current_round = self.get_current_round()?;

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
        if !chunk.acquire_lock(&participant_id) {
            return Err(CoordinatorError::FailedToAcquireLock);
        }

        // Attempt to update the current round chunk.
        if !current_round.set_chunk(chunk_id, &chunk) {
            return Err(CoordinatorError::FailedToUpdateChunk);
        }

        // Acquire the storage write lock.
        match self.storage.write() {
            Ok(mut storage) => match storage.insert(Key::CurrentRound, Value::CurrentRound(current_round)) {
                true => Ok(()),
                false => Err(CoordinatorError::FailedToUpdateChunk),
            },
            _ => return Err(CoordinatorError::StorageFailed),
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
}
