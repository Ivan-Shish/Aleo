use std::{collections::LinkedList, str::FromStr};

use serde::{
    de::{self, Error},
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
};
use thiserror::Error;

/// The identity/position of a task to be performed by a ceremony
/// participant at a given contribution level, for a given chunk.
///
/// Each contribution for a given task will be created by a unique
/// contributor. The total number of contributions per task at the end
/// of a successful round will be equal to the number of contributors
/// in that round.
///
/// ```txt, ignore
///   Contribution ID
/// +----------------+---------+---------+---------+
/// | ...            |  Task   |  Task   |  Task   |
/// +----------------+---------+---------+---------+
/// | Contribution 1 |  Task   |  Task   |  Task   |
/// +----------------+---------+---------+---------+
/// | Contribution 0 |  Task   |  Task   |  Task   |
/// +----------------+---------+---------+---------+
///                  | Chunk 0 | Chunk 1 | ...     | Chunk ID
///                  +---------+---------+---------+
/// ```
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Task {
    chunk_id: u64,
    contribution_id: u64,
}

impl Task {
    #[inline]
    pub fn new(chunk_id: u64, contribution_id: u64) -> Self {
        Self {
            chunk_id,
            contribution_id,
        }
    }

    #[inline]
    pub fn contains(&self, chunk_id: u64) -> bool {
        self.chunk_id == chunk_id
    }

    #[inline]
    pub fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    #[inline]
    pub fn contribution_id(&self) -> u64 {
        self.contribution_id
    }

    #[inline]
    pub fn to_tuple(&self) -> (u64, u64) {
        (self.chunk_id, self.contribution_id)
    }
}

impl Serialize for Task {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}/{}", self.chunk_id, self.contribution_id))
    }
}

impl<'de> Deserialize<'de> for Task {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Task, D::Error> {
        let s = String::deserialize(deserializer)?;

        let mut task = s.split("/");
        let chunk_id = task.next().ok_or(D::Error::custom("invalid chunk ID"))?;
        let contribution_id = task.next().ok_or(D::Error::custom("invalid contribution ID"))?;
        Ok(Task::new(
            u64::from_str(&chunk_id).map_err(de::Error::custom)?,
            u64::from_str(&contribution_id).map_err(de::Error::custom)?,
        ))
    }
}

#[derive(Debug, Error)]
pub enum TaskInitializationError {
    #[error(
        "The specified starting bucket id {starting_bucket_id} is out of range. \
        It should be less than number of contributors ({number_of_contributors})."
    )]
    StartingBucketIdOutOfRange {
        starting_bucket_id: u64,
        number_of_contributors: u64,
    },
    #[error(
        "The specified number of chunks ({number_of_chunks}) is too small. \
        It needs to be greater than or equal to the number of contributors \
        ({number_of_contributors})."
    )]
    NotEnoughChunks {
        number_of_chunks: u64,
        number_of_contributors: u64,
    },
}

/// Constructs ceremony tasks for a participant. The chunks are
/// divided up into buckets, ranges of chunks which each participant
/// will operate on exclusively before moving onto the next bucket
/// where it will build on another participant's contributions. The
/// number of buckets is equal to the `number_of_contributors` in the
/// round. The `starting_bucket_id` specifies which bucket the
/// participant will start in.
///
/// If the `number_of_contributors == 0` then an empty task list will
/// be returned.
///
/// # Constraints
///
/// The following contraints/relationships apply to the inputs of this
/// function if `number_of_contributors > 0`:
///
/// + `starting_bucket_id < number_of_contributors`
/// + `number_of_chunks >= number_of_contributors`
///
/// # Examples
///
/// With
///
/// + `starting_bucket_id = 0`
/// + `number_of_chunks = 4`
/// + `number_of_contributors = 2`
///
/// The following tasks will be initialized in this `Task N` order:
///
/// ```txt, ignore
///   Contribution ID
/// +----------------+                   +---------+---------+
/// | Contribution 1 |                   |  Task 2 |  Task 3 |
/// +----------------+---------+---------+---------+---------+
/// | Contribution 0 |  Task 0 |  Task 1 |
/// +----------------+---------+---------+---------+---------+
///                  | Chunk 0 | Chunk 1 | Chunk 2 | Chunk 3 | Chunk ID
///                  +---------+---------+---------+---------+
///                  |      Bucket 0     |      Bucket 1     |
/// ```
///
/// With
///
/// + `starting_bucket_id = 1`
/// + `number_of_chunks = 4`
/// + `number_of_contributors = 2`
///
/// The following tasks will be initialized in this `Task N` order:
///
/// ```txt, ignore
///   Contribution ID
/// +----------------+---------+---------+
/// | Contribution 1 |  Task 2 |  Task 3 |
/// +----------------+---------+---------+---------+---------+
/// | Contribution 0 |                   |  Task 0 |  Task 1 |
/// +----------------+---------+---------+---------+---------+
///                  | Chunk 0 | Chunk 1 | Chunk 2 | Chunk 3 | Chunk ID
///                  +---------+---------+---------+---------+
///                  |      Bucket 0     |      Bucket 1     |
/// ```
pub fn initialize_tasks(
    starting_bucket_id: u64, // 0-indexed
    number_of_chunks: u64,
    number_of_contributors: u64,
) -> Result<LinkedList<Task>, TaskInitializationError> {
    if number_of_contributors == 0 {
        return Ok(LinkedList::new());
    }

    // Check whether the starting bucket id is out of range
    if starting_bucket_id >= number_of_contributors {
        return Err(TaskInitializationError::StartingBucketIdOutOfRange {
            starting_bucket_id,
            number_of_contributors,
        });
    }

    // Check whether there are not enough chunks for the number of
    // contributors
    if number_of_chunks < number_of_contributors {
        return Err(TaskInitializationError::NotEnoughChunks {
            number_of_chunks,
            number_of_contributors,
        });
    }

    let number_of_buckets = number_of_contributors;

    // Fetch the bucket size.
    let bucket_size = number_of_chunks / number_of_buckets as u64;

    // It takes `number_of_contributors * bucket_size` to get to initial stable state.
    // You will jump up (total_jumps - starting_bucket_id) times.
    let number_of_initial_steps = (number_of_contributors - 1) - starting_bucket_id;
    let number_of_final_steps = starting_bucket_id;
    let mut initial_steps = 0;
    let mut final_steps = 0;

    // Compute the start and end indices.
    let start = starting_bucket_id * bucket_size;
    let end = start + number_of_chunks;

    // Add the tasks in FIFO ordering.
    let mut tasks = LinkedList::new();
    tasks.push_back(Task::new(start % number_of_chunks, 1));

    // Skip one from the start index and calculate the chunk ID and contribution ID to the end.
    for current_index in start + 1..end {
        let chunk_id = current_index % number_of_chunks;

        // Check if this is a new bucket.
        let is_new_bucket = (chunk_id % bucket_size) == 0;

        // Check if we have initial steps to increment.
        if is_new_bucket && initial_steps < number_of_initial_steps {
            initial_steps += 1;
        }

        // Check if we have iterated past the modulus and are in final steps.
        if is_new_bucket && current_index >= number_of_chunks {
            // Check if we have final steps to increment.
            if final_steps < number_of_final_steps {
                final_steps += 1;
            }
        }

        // Compute the contribution ID.
        let steps = (initial_steps + final_steps) % number_of_contributors;
        let contribution_id = steps + 1;

        tasks.push_back(Task::new(chunk_id, contribution_id as u64));
    }

    assert!(tasks.len() == number_of_chunks as usize);

    Ok(tasks)
}

#[cfg(test)]
mod test {
    use super::{initialize_tasks, Task, TaskInitializationError};
    use crate::testing::prelude::test_logger;
    use std::collections::HashSet;

    #[test]
    fn test_task() {
        let task = Task::new(0, 1);
        assert_eq!("\"0/1\"", serde_json::to_string(&task).unwrap());
        assert_eq!(task, serde_json::from_str("\"0/1\"").unwrap());
    }

    #[test]
    fn test_initialize_tasks_2_chunks_1_contributor() {
        let number_of_chunks = 2;
        let number_of_contributors = 1;

        let bucket_id = 0;
        let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
            .unwrap()
            .into_iter();
        assert_eq!(Some(Task::new(0, 1)), tasks.next());
        assert_eq!(Some(Task::new(1, 1)), tasks.next());
        assert!(tasks.next().is_none());

        let bucket_id = 1;
        match initialize_tasks(bucket_id, number_of_chunks, number_of_contributors).unwrap_err() {
            TaskInitializationError::StartingBucketIdOutOfRange {
                starting_bucket_id,
                number_of_contributors,
            } => {
                assert_eq!(1, starting_bucket_id);
                assert_eq!(1, number_of_contributors);
            }
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_initialize_tasks_1_chunk_2_contributors() {
        let number_of_chunks = 1;
        let number_of_contributors = 2;

        let bucket_id = 0;
        match initialize_tasks(bucket_id, number_of_chunks, number_of_contributors).unwrap_err() {
            TaskInitializationError::NotEnoughChunks {
                number_of_chunks,
                number_of_contributors,
            } => {
                assert_eq!(1, number_of_chunks);
                assert_eq!(2, number_of_contributors);
            }
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_initialize_tasks_2_chunks_2_contributors() {
        let number_of_chunks = 2;
        let number_of_contributors = 2;

        let bucket_id = 0;
        let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
            .unwrap()
            .into_iter();
        assert_eq!(Some(Task::new(0, 1)), tasks.next());
        assert_eq!(Some(Task::new(1, 2)), tasks.next());
        assert!(tasks.next().is_none());

        let bucket_id = 1;
        let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
            .unwrap()
            .into_iter();
        assert_eq!(Some(Task::new(1, 1)), tasks.next());
        assert_eq!(Some(Task::new(0, 2)), tasks.next());
        assert!(tasks.next().is_none());

        let bucket_id = 2;
        match initialize_tasks(bucket_id, number_of_chunks, number_of_contributors).unwrap_err() {
            TaskInitializationError::StartingBucketIdOutOfRange {
                starting_bucket_id,
                number_of_contributors,
            } => {
                assert_eq!(2, starting_bucket_id);
                assert_eq!(2, number_of_contributors);
            }
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_initialize_tasks_3_chunks_2_contributors() {
        let number_of_chunks = 3;
        let number_of_contributors = 2;

        let bucket_id = 0;
        let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
            .unwrap()
            .into_iter();
        assert_eq!(Some(Task::new(0, 1)), tasks.next());
        assert_eq!(Some(Task::new(1, 2)), tasks.next());
        assert_eq!(Some(Task::new(2, 2)), tasks.next());
        assert!(tasks.next().is_none());

        let bucket_id = 1;
        let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
            .unwrap()
            .into_iter();
        assert_eq!(Some(Task::new(1, 1)), tasks.next());
        assert_eq!(Some(Task::new(2, 1)), tasks.next());
        assert_eq!(Some(Task::new(0, 2)), tasks.next());
        assert!(tasks.next().is_none());
    }

    #[test]
    fn test_initialize_tasks_unique() {
        test_logger();

        fn test_uniqueness_of_tasks(number_of_chunks: u64, number_of_contributors: u64) {
            let mut all_tasks = HashSet::new();
            for bucket_id in 0..number_of_contributors {
                // trace!("Contributor {}", bucket_id);
                let mut tasks = initialize_tasks(bucket_id, number_of_chunks, number_of_contributors)
                    .unwrap()
                    .into_iter();
                while let Some(task) = tasks.next() {
                    assert!(all_tasks.insert(task));
                }
            }
        }

        for number_of_contributors in 1..32 {
            tracing::trace!("{} contributors", number_of_contributors,);
            for number_of_chunks in number_of_contributors..256 {
                test_uniqueness_of_tasks(number_of_chunks, number_of_contributors);
            }
        }
    }
}
