//! Stable interfaces shared by the MAMCLI runtime and adapter crates.

pub mod store;

pub use store::{
    AgentName, ConversationStore, CreateSession, EventKind, EventSequence, NewEvent, Session,
    SessionId, SessionQuery, SessionSummary, SessionVersion, StoreError, StoredEvent,
};
