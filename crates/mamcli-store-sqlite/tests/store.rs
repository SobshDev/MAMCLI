use mamcli_interface::{
    AgentName, ConversationStore, CreateSession, EventSequence, NewEvent, SessionId, SessionQuery,
    SessionVersion, StoreError,
};
use mamcli_store_sqlite::SqliteConversationStore;
use serde_json::json;

#[tokio::test]
async fn persists_sessions_and_versioned_events() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("mamcli.db");
    let session_id = SessionId::from("ses_test");

    {
        let store = SqliteConversationStore::open(&database).await.unwrap();
        let created = store
            .create_session(
                CreateSession::new("architect")
                    .with_id(session_id.clone())
                    .with_title("Authentication API")
                    .with_metadata(json!({"model": "openai/example"})),
            )
            .await
            .unwrap();

        assert_eq!(created.version, SessionVersion::INITIAL);
        assert_eq!(created.root_agent, AgentName::from("architect"));

        let version = store
            .append_events(
                &session_id,
                SessionVersion::INITIAL,
                vec![
                    NewEvent::new("message_added", json!({"role": "user", "text": "Hello"})),
                    NewEvent::new("message_added", json!({"role": "assistant", "text": "Hi"})),
                ],
            )
            .await
            .unwrap();

        assert_eq!(version, SessionVersion(2));

        let events = store
            .read_events(&session_id, Some(EventSequence(1)))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, EventSequence(2));
        assert_eq!(events[0].kind.as_str(), "message_added");
        assert_eq!(events[0].payload["text"], "Hi");
    }

    let reopened = SqliteConversationStore::open(&database).await.unwrap();
    let session = reopened.get_session(&session_id).await.unwrap().unwrap();
    assert_eq!(session.version, SessionVersion(2));
    assert_eq!(session.metadata["model"], "openai/example");

    let latest = reopened
        .latest_session(&AgentName::from("architect"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.id, session_id);

    let listed = reopened
        .list_sessions(SessionQuery {
            root_agent: Some(AgentName::from("architect")),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].version, SessionVersion(2));
}

#[tokio::test]
async fn rejects_stale_writes_without_appending_events() {
    let store = SqliteConversationStore::in_memory().await.unwrap();
    let session = store
        .create_session(CreateSession::new("architect"))
        .await
        .unwrap();

    store
        .append_events(
            &session.id,
            SessionVersion::INITIAL,
            vec![NewEvent::new("first", json!({"value": 1}))],
        )
        .await
        .unwrap();

    let error = store
        .append_events(
            &session.id,
            SessionVersion::INITIAL,
            vec![NewEvent::new("stale", json!({"value": 2}))],
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        StoreError::Conflict {
            expected: SessionVersion(0),
            actual: SessionVersion(1),
        }
    ));

    let events = store.read_events(&session.id, None).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind.as_str(), "first");
}

#[tokio::test]
async fn deletion_cascades_events_and_missing_sessions_are_reported() {
    let store = SqliteConversationStore::in_memory().await.unwrap();
    let session = store
        .create_session(CreateSession::new("reviewer"))
        .await
        .unwrap();

    store
        .append_events(
            &session.id,
            SessionVersion::INITIAL,
            vec![NewEvent::new("message_added", json!({"text": "Review"}))],
        )
        .await
        .unwrap();

    store.delete_session(&session.id).await.unwrap();
    assert!(store.get_session(&session.id).await.unwrap().is_none());

    let error = store.read_events(&session.id, None).await.unwrap_err();
    assert!(matches!(error, StoreError::NotFound(id) if id == session.id));
}
