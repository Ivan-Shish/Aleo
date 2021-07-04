use crate::{
    objects::LockResponse,
    utils::{read_from_file, write_to_file},
};

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use tracing::warn;

///
/// The list of chunks that the verifier needs to verify.
///
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Tasks {
    /// Ordered queue of lock response tasks.
    pub(crate) queue: VecDeque<LockResponse>,
}

impl Tasks {
    ///
    /// Returns the list of pending tasks.
    ///
    pub fn get_tasks(&self) -> &VecDeque<LockResponse> {
        &self.queue
    }

    ///
    /// Returns `true` if there are no tasks in the queue.
    /// Otherwise, return `false`
    ///
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    ///
    /// Returns and removes the first task in the queue.
    /// If there are no tasks in the queue, returns `None`.
    ///
    pub fn next_task(&mut self) -> Option<LockResponse> {
        self.queue.pop_front()
    }

    ///
    /// Add a task to the queue if it doesn't already exist.
    ///
    pub fn add_task(&mut self, task: LockResponse) {
        if !self.queue.contains(&task) {
            self.queue.push_back(task);
        }
    }

    ///
    /// Removes a task from the queue if it exists.
    ///
    pub fn remove_task(&mut self, task: &LockResponse) {
        self.queue.retain(|t| t != task);
    }

    ///
    /// Read tasks from a stored file. Returns a list of empty tasks if
    /// the file could not be read.
    ///
    pub fn load(file_path: &str) -> Self {
        // Read tasks from a file
        match read_from_file(file_path) {
            Ok(file) => match serde_json::from_slice(&file) {
                Ok(tasks) => tasks,
                Err(err) => {
                    warn!("Failed to read tasks from {} {}", file_path, err);
                    Tasks::default()
                }
            },
            Err(_) => Tasks::default(),
        }
    }

    ///
    /// Writes the current tasks to disk if there are tasks in the queue.
    ///
    pub fn store(&self, file_path: &str) -> anyhow::Result<()> {
        if !self.is_empty() {
            // Write tasks to disk.
            let task_bytes = serde_json::to_vec_pretty(&self)?;
            write_to_file(file_path, task_bytes)?;
        }

        Ok(())
    }
}

impl std::default::Default for Tasks {
    fn default() -> Self {
        Self { queue: VecDeque::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::remove_file_if_exists;

    use lazy_static::lazy_static;
    use std::path::Path;

    const TEST_TASK_FILE: &str = "TEST.tasks";

    lazy_static! {
        pub static ref TASK_1: LockResponse = LockResponse {
            chunk_id: 1,
            locked: true,
            participant_id: "test_participant_1".to_string(),
            previous_response_locator: "test_previous_response_locator_1".to_string(),
            challenge_locator: "test_challenge_locator_1".to_string(),
            response_locator: "test_response_locator_1".to_string(),
        };
        pub static ref TASK_2: LockResponse = LockResponse {
            chunk_id: 2,
            locked: true,
            participant_id: "test_participant_2".to_string(),
            previous_response_locator: "test_previous_response_locator_2".to_string(),
            challenge_locator: "test_challenge_locator_2".to_string(),
            response_locator: "test_response_locator_2".to_string(),
        };
        pub static ref TASK_3: LockResponse = LockResponse {
            chunk_id: 3,
            locked: true,
            participant_id: "test_participant_3".to_string(),
            previous_response_locator: "test_previous_response_locator_3".to_string(),
            challenge_locator: "test_challenge_locator_3".to_string(),
            response_locator: "test_response_locator_3".to_string(),
        };
    }

    #[test]
    pub fn test_assign_tasks() {
        let mut tasks = Tasks::default();
        assert_eq!(0, tasks.get_tasks().len());

        tasks.add_task(TASK_1.clone());
        assert_eq!(1, tasks.get_tasks().len());
        assert!(tasks.get_tasks().contains(&TASK_1));

        tasks.add_task(TASK_2.clone());
        assert_eq!(2, tasks.get_tasks().len());
        assert!(tasks.get_tasks().contains(&TASK_2));

        tasks.add_task(TASK_3.clone());
        assert_eq!(3, tasks.get_tasks().len());
        assert!(tasks.get_tasks().contains(&TASK_3));
    }

    #[test]
    pub fn test_next_tasks() {
        let mut tasks = Tasks::default();

        tasks.add_task(TASK_1.clone());
        tasks.add_task(TASK_2.clone());
        tasks.add_task(TASK_3.clone());

        assert_eq!(&*TASK_1, &tasks.next_task().unwrap());
        assert_eq!(&*TASK_2, &tasks.next_task().unwrap());
        assert_eq!(&*TASK_3, &tasks.next_task().unwrap());

        // Check that the tasks have looped.
        assert_eq!(0, tasks.get_tasks().len());
    }

    #[test]
    pub fn test_remove_tasks() {
        let mut tasks = Tasks::default();

        tasks.add_task(TASK_1.clone());
        tasks.add_task(TASK_2.clone());
        tasks.add_task(TASK_3.clone());

        tasks.remove_task(&TASK_1);
        assert_eq!(2, tasks.get_tasks().len());
        assert!(!tasks.get_tasks().contains(&TASK_1));

        tasks.remove_task(&TASK_2);
        assert_eq!(1, tasks.get_tasks().len());
        assert!(!tasks.get_tasks().contains(&TASK_2));

        tasks.remove_task(&TASK_3);
        assert_eq!(0, tasks.get_tasks().len());
        assert!(!tasks.get_tasks().contains(&TASK_3));
    }

    #[test]
    pub fn test_store_tasks() {
        remove_file_if_exists(TEST_TASK_FILE).unwrap();
        let mut tasks = Tasks::default();

        tasks.add_task(TASK_1.clone());
        tasks.add_task(TASK_2.clone());
        tasks.add_task(TASK_3.clone());

        let path = Path::new(TEST_TASK_FILE);

        assert!(!path.exists());
        assert!(tasks.store(TEST_TASK_FILE).is_ok());
        assert!(path.exists());

        remove_file_if_exists(TEST_TASK_FILE).unwrap();
    }

    #[test]
    pub fn test_load_tasks() {
        let mut tasks = Tasks::default();

        tasks.add_task(TASK_1.clone());
        tasks.add_task(TASK_2.clone());

        assert!(tasks.store(TEST_TASK_FILE).is_ok());
        let loaded_tasks = Tasks::load(TEST_TASK_FILE);
        assert_eq!(tasks, loaded_tasks);

        tasks.add_task(TASK_3.clone());

        assert!(tasks.store(TEST_TASK_FILE).is_ok());
        let loaded_tasks = Tasks::load(TEST_TASK_FILE);
        assert_eq!(tasks, loaded_tasks);

        remove_file_if_exists(TEST_TASK_FILE).unwrap();
    }
}
