//! SQLite implementation of [`mamcli_interface::ConversationStore`].

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use mamcli_interface::{
    AgentName, ConversationStore, CreateSession, EventKind, EventSequence, NewEvent, Session,
    SessionId, SessionQuery, SessionSummary, SessionVersion, StoreError, StoredEvent,
};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};

static MIGRATOR: Migrator = sqlx::migrate!();

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct SqliteConversationStore {
    pool: SqlitePool,
}

impl SqliteConversationStore {
    /// Opens or creates a file-backed SQLite store and applies migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                StoreError::Database(format!(
                    "could not create database directory `{}`: {error}",
                    parent.display()
                ))
            })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);

        Self::connect(options, 5).await
    }

    /// Creates an isolated in-memory store, primarily for tests and ephemeral runs.
    pub async fn in_memory() -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);

        // Separate connections receive separate `:memory:` databases.
        Self::connect(options, 1).await
    }

    async fn connect(
        options: SqliteConnectOptions,
        max_connections: u32,
    ) -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .map_err(database_error)?;

        MIGRATOR.run(&pool).await.map_err(|error| {
            StoreError::Database(format!("could not migrate SQLite database: {error}"))
        })?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl ConversationStore for SqliteConversationStore {
    async fn create_session(&self, input: CreateSession) -> Result<Session, StoreError> {
        validate_non_empty("session id", input.id.as_str())?;
        validate_non_empty("root agent", input.root_agent.as_str())?;

        let now = Utc::now();
        let encoded_now = encode_timestamp(now);
        let metadata = serde_json::to_string(&input.metadata).map_err(|error| {
            StoreError::InvalidInput(format!("session metadata could not be serialized: {error}"))
        })?;

        let result = sqlx::query(
            r#"
            INSERT INTO sessions (
                id, root_agent, title, metadata, version, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 0, ?, ?)
            "#,
        )
        .bind(input.id.as_str())
        .bind(input.root_agent.as_str())
        .bind(input.title.as_deref())
        .bind(metadata)
        .bind(&encoded_now)
        .bind(&encoded_now)
        .execute(&self.pool)
        .await;

        if let Err(error) = result {
            if let sqlx::Error::Database(database_error) = &error
                && database_error.is_unique_violation()
            {
                return Err(StoreError::AlreadyExists(input.id));
            }

            return Err(database_error(error));
        }

        Ok(Session {
            id: input.id,
            root_agent: input.root_agent,
            title: input.title,
            metadata: input.metadata,
            version: SessionVersion::INITIAL,
            created_at: now,
            updated_at: now,
        })
    }

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, root_agent, title, metadata, version, created_at, updated_at
            FROM sessions
            WHERE id = ?
            "#,
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;

        row.as_ref().map(session_from_row).transpose()
    }

    async fn latest_session(&self, root_agent: &AgentName) -> Result<Option<Session>, StoreError> {
        let row = sqlx::query(
            r#"
            SELECT id, root_agent, title, metadata, version, created_at, updated_at
            FROM sessions
            WHERE root_agent = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            "#,
        )
        .bind(root_agent.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;

        row.as_ref().map(session_from_row).transpose()
    }

    async fn list_sessions(&self, query: SessionQuery) -> Result<Vec<SessionSummary>, StoreError> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }

        let limit = i64::try_from(query.limit)
            .map_err(|_| StoreError::InvalidInput("session limit is too large".to_owned()))?;

        let rows = if let Some(root_agent) = query.root_agent {
            sqlx::query(
                r#"
                SELECT id, root_agent, title, version, created_at, updated_at
                FROM sessions
                WHERE root_agent = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                "#,
            )
            .bind(root_agent.as_str())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT id, root_agent, title, version, created_at, updated_at
                FROM sessions
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(database_error)?;

        rows.iter().map(session_summary_from_row).collect()
    }

    async fn read_events(
        &self,
        session_id: &SessionId,
        after: Option<EventSequence>,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        ensure_session_exists(&self.pool, session_id).await?;

        let after = after.unwrap_or(EventSequence(0));
        let after = integer_to_sql("event sequence", after.0)?;

        let rows = sqlx::query(
            r#"
            SELECT sequence, kind, payload, created_at
            FROM events
            WHERE session_id = ? AND sequence > ?
            ORDER BY sequence ASC
            "#,
        )
        .bind(session_id.as_str())
        .bind(after)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;

        rows.iter()
            .map(|row| event_from_row(session_id, row))
            .collect()
    }

    async fn append_events(
        &self,
        session_id: &SessionId,
        expected_version: SessionVersion,
        events: Vec<NewEvent>,
    ) -> Result<SessionVersion, StoreError> {
        if events.is_empty() {
            return Err(StoreError::InvalidInput(
                "an event batch cannot be empty".to_owned(),
            ));
        }

        for event in &events {
            validate_non_empty("event kind", event.kind.as_str())?;
        }

        let event_count = u64::try_from(events.len())
            .map_err(|_| StoreError::InvalidInput("event batch is too large".to_owned()))?;
        let new_version = expected_version
            .0
            .checked_add(event_count)
            .map(SessionVersion)
            .ok_or_else(|| StoreError::InvalidInput("session version overflow".to_owned()))?;

        let expected_sql = integer_to_sql("session version", expected_version.0)?;
        let new_version_sql = integer_to_sql("session version", new_version.0)?;
        let encoded_events = events
            .into_iter()
            .map(|event| {
                let payload = serde_json::to_string(&event.payload).map_err(|error| {
                    StoreError::InvalidInput(format!(
                        "event `{}` could not be serialized: {error}",
                        event.kind
                    ))
                })?;
                Ok((event.kind, payload))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;

        let now = encode_timestamp(Utc::now());
        let mut transaction = self.pool.begin().await.map_err(database_error)?;

        let update = sqlx::query(
            r#"
            UPDATE sessions
            SET version = ?, updated_at = ?
            WHERE id = ? AND version = ?
            "#,
        )
        .bind(new_version_sql)
        .bind(&now)
        .bind(session_id.as_str())
        .bind(expected_sql)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;

        if update.rows_affected() == 0 {
            let actual = sqlx::query("SELECT version FROM sessions WHERE id = ?")
                .bind(session_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(database_error)?;

            transaction.rollback().await.map_err(database_error)?;

            return match actual {
                Some(row) => {
                    let actual = sql_to_integer(
                        "session version",
                        row.try_get::<i64, _>("version").map_err(database_error)?,
                    )?;
                    Err(StoreError::Conflict {
                        expected: expected_version,
                        actual: SessionVersion(actual),
                    })
                }
                None => Err(StoreError::NotFound(session_id.clone())),
            };
        }

        for (offset, (kind, payload)) in encoded_events.into_iter().enumerate() {
            let offset = u64::try_from(offset)
                .map_err(|_| StoreError::InvalidInput("event batch is too large".to_owned()))?;
            let sequence = expected_version
                .0
                .checked_add(offset + 1)
                .ok_or_else(|| StoreError::InvalidInput("event sequence overflow".to_owned()))?;
            let sequence = integer_to_sql("event sequence", sequence)?;

            sqlx::query(
                r#"
                INSERT INTO events (session_id, sequence, kind, payload, created_at)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(session_id.as_str())
            .bind(sequence)
            .bind(kind.as_str())
            .bind(payload)
            .bind(&now)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }

        transaction.commit().await.map_err(database_error)?;
        Ok(new_version)
    }

    async fn delete_session(&self, id: &SessionId) -> Result<(), StoreError> {
        let result = sqlx::query("DELETE FROM sessions WHERE id = ?")
            .bind(id.as_str())
            .execute(&self.pool)
            .await
            .map_err(database_error)?;

        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound(id.clone()));
        }

        Ok(())
    }
}

fn session_from_row(row: &SqliteRow) -> Result<Session, StoreError> {
    let metadata = row
        .try_get::<String, _>("metadata")
        .map_err(database_error)
        .and_then(|metadata| decode_json("session metadata", &metadata))?;

    Ok(Session {
        id: SessionId::from(row.try_get::<String, _>("id").map_err(database_error)?),
        root_agent: AgentName::from(
            row.try_get::<String, _>("root_agent")
                .map_err(database_error)?,
        ),
        title: row
            .try_get::<Option<String>, _>("title")
            .map_err(database_error)?,
        metadata,
        version: SessionVersion(sql_to_integer(
            "session version",
            row.try_get::<i64, _>("version").map_err(database_error)?,
        )?),
        created_at: decode_timestamp(
            "session created_at",
            &row.try_get::<String, _>("created_at")
                .map_err(database_error)?,
        )?,
        updated_at: decode_timestamp(
            "session updated_at",
            &row.try_get::<String, _>("updated_at")
                .map_err(database_error)?,
        )?,
    })
}

fn session_summary_from_row(row: &SqliteRow) -> Result<SessionSummary, StoreError> {
    Ok(SessionSummary {
        id: SessionId::from(row.try_get::<String, _>("id").map_err(database_error)?),
        root_agent: AgentName::from(
            row.try_get::<String, _>("root_agent")
                .map_err(database_error)?,
        ),
        title: row
            .try_get::<Option<String>, _>("title")
            .map_err(database_error)?,
        version: SessionVersion(sql_to_integer(
            "session version",
            row.try_get::<i64, _>("version").map_err(database_error)?,
        )?),
        created_at: decode_timestamp(
            "session created_at",
            &row.try_get::<String, _>("created_at")
                .map_err(database_error)?,
        )?,
        updated_at: decode_timestamp(
            "session updated_at",
            &row.try_get::<String, _>("updated_at")
                .map_err(database_error)?,
        )?,
    })
}

fn event_from_row(session_id: &SessionId, row: &SqliteRow) -> Result<StoredEvent, StoreError> {
    let payload = row
        .try_get::<String, _>("payload")
        .map_err(database_error)
        .and_then(|payload| decode_json("event payload", &payload))?;

    Ok(StoredEvent {
        session_id: session_id.clone(),
        sequence: EventSequence(sql_to_integer(
            "event sequence",
            row.try_get::<i64, _>("sequence").map_err(database_error)?,
        )?),
        kind: EventKind::from(row.try_get::<String, _>("kind").map_err(database_error)?),
        payload,
        created_at: decode_timestamp(
            "event created_at",
            &row.try_get::<String, _>("created_at")
                .map_err(database_error)?,
        )?,
    })
}

async fn ensure_session_exists(
    pool: &SqlitePool,
    session_id: &SessionId,
) -> Result<(), StoreError> {
    let exists = sqlx::query("SELECT 1 FROM sessions WHERE id = ?")
        .bind(session_id.as_str())
        .fetch_optional(pool)
        .await
        .map_err(database_error)?
        .is_some();

    if exists {
        Ok(())
    } else {
        Err(StoreError::NotFound(session_id.clone()))
    }
}

fn encode_timestamp(timestamp: DateTime<Utc>) -> String {
    // Fixed-width fractional seconds preserve both round-trip equality and
    // lexicographic ordering in SQLite.
    timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn decode_timestamp(field: &str, encoded: &str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(encoded)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            StoreError::InvalidData(format!(
                "{field} contains invalid timestamp `{encoded}`: {error}"
            ))
        })
}

fn decode_json(field: &str, encoded: &str) -> Result<Value, StoreError> {
    serde_json::from_str(encoded)
        .map_err(|error| StoreError::InvalidData(format!("{field} contains invalid JSON: {error}")))
}

fn integer_to_sql(field: &str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::InvalidInput(format!("{field} exceeds SQLite's integer range")))
}

fn sql_to_integer(field: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::InvalidData(format!("{field} cannot be negative")))
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        Err(StoreError::InvalidInput(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn database_error(error: sqlx::Error) -> StoreError {
    StoreError::Database(error.to_string())
}
