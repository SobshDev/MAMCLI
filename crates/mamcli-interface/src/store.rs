//! Provider-independent persistence contracts for conversation sessions.

use std::fmt::{self, Display, Formatter};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use ulid::Ulid;

macro_rules! string_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }
    };
}

string_newtype!(SessionId);
string_newtype!(AgentName);
string_newtype!(EventKind);

impl SessionId {
    /// Creates a lexicographically sortable session identifier.
    pub fn new() -> Self {
        Self(format!("ses_{}", Ulid::new()))
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// The last event sequence durably appended to a session.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SessionVersion(pub u64);

impl SessionVersion {
    pub const INITIAL: Self = Self(0);
}

impl Display for SessionVersion {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// A one-based sequence number within a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSequence(pub u64);

impl Display for EventSequence {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub root_agent: AgentName,
    pub title: Option<String>,
    /// A runtime-owned snapshot of configuration needed to resume the session.
    pub metadata: Value,
    pub version: SessionVersion,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: SessionId,
    pub root_agent: AgentName,
    pub title: Option<String>,
    pub version: SessionVersion,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateSession {
    pub id: SessionId,
    pub root_agent: AgentName,
    pub title: Option<String>,
    pub metadata: Value,
}

impl CreateSession {
    pub fn new(root_agent: impl Into<AgentName>) -> Self {
        Self {
            id: SessionId::new(),
            root_agent: root_agent.into(),
            title: None,
            metadata: Value::Object(Default::default()),
        }
    }

    pub fn with_id(mut self, id: impl Into<SessionId>) -> Self {
        self.id = id.into();
        self
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionQuery {
    pub root_agent: Option<AgentName>,
    pub limit: usize,
}

impl Default for SessionQuery {
    fn default() -> Self {
        Self {
            root_agent: None,
            limit: 50,
        }
    }
}

/// An event before the store assigns its sequence and timestamp.
///
/// The envelope is intentionally generic. Runtime and plugin crates own the
/// schemas of their payloads, while every store can persist them unchanged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewEvent {
    pub kind: EventKind,
    pub payload: Value,
}

impl NewEvent {
    pub fn new(kind: impl Into<EventKind>, payload: Value) -> Self {
        Self {
            kind: kind.into(),
            payload,
        }
    }

    pub fn from_serializable<T>(
        kind: impl Into<EventKind>,
        payload: &T,
    ) -> Result<Self, serde_json::Error>
    where
        T: Serialize + ?Sized,
    {
        Ok(Self::new(kind, serde_json::to_value(payload)?))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub session_id: SessionId,
    pub sequence: EventSequence,
    pub kind: EventKind,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("session `{0}` was not found")]
    NotFound(SessionId),

    #[error("session `{0}` already exists")]
    AlreadyExists(SessionId),

    #[error(
        "session version conflict: expected version {expected}, but current version is {actual}"
    )]
    Conflict {
        expected: SessionVersion,
        actual: SessionVersion,
    },

    #[error("invalid store input: {0}")]
    InvalidInput(String),

    #[error("stored data is invalid: {0}")]
    InvalidData(String),

    #[error("database error: {0}")]
    Database(String),
}

#[async_trait]
pub trait ConversationStore: Send + Sync {
    async fn create_session(&self, input: CreateSession) -> Result<Session, StoreError>;

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError>;

    async fn latest_session(&self, root_agent: &AgentName) -> Result<Option<Session>, StoreError>;

    async fn list_sessions(&self, query: SessionQuery) -> Result<Vec<SessionSummary>, StoreError>;

    /// Returns events strictly after `after`, ordered by sequence.
    ///
    /// Returns [`StoreError::NotFound`] when the session does not exist.
    async fn read_events(
        &self,
        session_id: &SessionId,
        after: Option<EventSequence>,
    ) -> Result<Vec<StoredEvent>, StoreError>;

    /// Atomically appends a non-empty event batch.
    ///
    /// `expected_version` must equal the session's current version. The returned
    /// version is the sequence assigned to the final event in the batch.
    async fn append_events(
        &self,
        session_id: &SessionId,
        expected_version: SessionVersion,
        events: Vec<NewEvent>,
    ) -> Result<SessionVersion, StoreError>;

    async fn delete_session(&self, id: &SessionId) -> Result<(), StoreError>;
}
