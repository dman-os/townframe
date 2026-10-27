CREATE TABLE IF NOT EXISTS pauperfuse_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS pauperfuse_object_state (
    doc_id TEXT PRIMARY KEY,
    relative_path TEXT NOT NULL,
    provider_hash TEXT,
    backend_hash TEXT
) STRICT;
