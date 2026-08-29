CREATE TABLE big_repo_subduction_trees (
      scope_id INTEGER NOT NULL
      ,sedimentree_id BLOB NOT NULL
    , PRIMARY KEY(scope_id, sedimentree_id)
) STRICT;

CREATE TABLE big_repo_subduction_commits (
      scope_id INTEGER NOT NULL
      ,sedimentree_id BLOB NOT NULL
      ,commit_id BLOB NOT NULL
      ,digest BLOB NOT NULL
      ,signed BLOB NOT NULL
      ,blob BLOB NOT NULL
      ,PRIMARY KEY(scope_id, sedimentree_id, commit_id, digest)
    , FOREIGN KEY(scope_id, sedimentree_id)
        REFERENCES big_repo_subduction_trees(scope_id, sedimentree_id)
) STRICT;

CREATE TABLE big_repo_subduction_fragments (
      scope_id INTEGER NOT NULL
      ,sedimentree_id BLOB NOT NULL
      ,head_id BLOB NOT NULL
      ,digest BLOB NOT NULL
      ,signed BLOB NOT NULL
      ,blob BLOB NOT NULL
      ,PRIMARY KEY(scope_id, sedimentree_id, head_id, digest)
    , FOREIGN KEY(scope_id, sedimentree_id)
        REFERENCES big_repo_subduction_trees(scope_id, sedimentree_id)
) STRICT;

CREATE TABLE big_repo_keyhive_event_log (
      scope_id INTEGER NOT NULL
      ,seq INTEGER NOT NULL
      ,event_hash BLOB NOT NULL
      ,event_bytes BLOB NOT NULL
      ,event_kind INTEGER
      ,source_id BLOB
      ,PRIMARY KEY(scope_id, seq)
    , UNIQUE(scope_id, event_hash)
 ) STRICT;

CREATE TABLE big_repo_keyhive_admissions (
      scope_id INTEGER NOT NULL
      ,seq INTEGER NOT NULL
      ,event_hash BLOB NOT NULL
      ,source_id BLOB
      ,PRIMARY KEY(scope_id, seq)
    , UNIQUE(scope_id, event_hash)
 ) STRICT;

CREATE TABLE big_repo_keyhive_archived_through (
      scope_id INTEGER PRIMARY KEY
      ,seq INTEGER NOT NULL DEFAULT 0
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
 ) STRICT;

CREATE TABLE big_repo_sync_commits_watermark (
      scope_id INTEGER NOT NULL
      ,doc_id BLOB NOT NULL
      ,big_sync_txid INTEGER NOT NULL
      ,latest_commit_row_id INTEGER NOT NULL
      ,PRIMARY KEY(scope_id, doc_id, big_sync_txid)
    , FOREIGN KEY(scope_id) REFERENCES big_sync_scopes(scope_id)
 ) STRICT;

CREATE TABLE big_repo_causal_ciphertext_index (
      scope_id INTEGER NOT NULL
      ,sedimentree_id BLOB NOT NULL
      ,content_ref BLOB NOT NULL
      ,digest BLOB NOT NULL
      ,kind INTEGER NOT NULL
      ,pcs_update_hash BLOB NOT NULL
    , PRIMARY KEY(scope_id, sedimentree_id, content_ref, digest, kind)
 ) STRICT;

CREATE INDEX big_repo_keyhive_event_log_hash_idx
    ON big_repo_keyhive_event_log(scope_id, event_hash);
CREATE INDEX big_repo_keyhive_admissions_seq_idx
    ON big_repo_keyhive_admissions(scope_id, seq);
CREATE INDEX big_repo_causal_ciphertext_update_idx
    ON big_repo_causal_ciphertext_index(scope_id, sedimentree_id, pcs_update_hash);

CREATE TABLE cursors (
    reader TEXT PRIMARY KEY
  , seq INTEGER NOT NULL
) STRICT;

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
