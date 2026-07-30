CREATE TABLE sessions (
    id              TEXT PRIMARY KEY NOT NULL,
    root_agent      TEXT NOT NULL,
    title           TEXT,
    metadata        TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX sessions_updated_at
    ON sessions(updated_at DESC);

CREATE INDEX sessions_root_agent_updated_at
    ON sessions(root_agent, updated_at DESC);

CREATE TABLE events (
    session_id      TEXT NOT NULL,
    sequence        INTEGER NOT NULL CHECK (sequence > 0),
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TEXT NOT NULL,

    PRIMARY KEY (session_id, sequence),
    FOREIGN KEY (session_id)
        REFERENCES sessions(id)
        ON DELETE CASCADE
);
