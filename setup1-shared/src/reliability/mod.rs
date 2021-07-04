//! Data structures related to reliability score checks

mod contributor;
mod coordinator;
mod message;

pub use contributor::ContributorMessageName;
pub use coordinator::CoordinatorMessageName;
use message::Message;
pub use message::{MessageName, MAXIMUM_MESSAGE_SIZE};

pub type ContributorMessage = Message<ContributorMessageName>;
pub type CoordinatorMessage = Message<CoordinatorMessageName>;
