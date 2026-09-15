CREATE TABLE big_repo_keyhive_admission_readers (
      scope_id INTEGER NOT NULL
      ,reader TEXT NOT NULL
      ,seq INTEGER NOT NULL DEFAULT 0
    , PRIMARY KEY(scope_id, reader)
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
) STRICT;

CREATE TABLE big_repo_keyhive_event_tombstones (
      scope_id INTEGER NOT NULL
      ,event_hash BLOB NOT NULL
    , PRIMARY KEY(scope_id, event_hash)
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
) STRICT;

CREATE INDEX big_repo_keyhive_admission_readers_scope_idx
    ON big_repo_keyhive_admission_readers(scope_id, seq);
