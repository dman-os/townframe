CREATE TABLE IF NOT EXISTS drawer_local_branches (
    doc_id TEXT NOT NULL
  , branch_path TEXT NOT NULL
  , branch_doc_id BLOB NOT NULL
  , vtag_version TEXT NOT NULL
  , vtag_actor_id TEXT NOT NULL
  , updated_at INTEGER NOT NULL
  , PRIMARY KEY (doc_id, branch_path)
) STRICT;
CREATE TABLE IF NOT EXISTS drawer_local_branches_deleted (
    id INTEGER PRIMARY KEY AUTOINCREMENT
  , doc_id TEXT NOT NULL
  , branch_path TEXT NOT NULL
  , branch_doc_id BLOB NOT NULL
  , branch_heads_json TEXT NOT NULL
  , vtag_version TEXT NOT NULL
  , vtag_actor_id TEXT NOT NULL
  , deleted_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_doc_id
    ON drawer_local_branches(doc_id);
CREATE INDEX IF NOT EXISTS idx_drawer_local_branches_deleted_doc_path
    ON drawer_local_branches_deleted(doc_id, branch_path, deleted_at DESC);

-- Write-ahead rows for drawer adds; see `drawer/outbox.rs` for the state
-- machine. Pending rows are never aged out, `done` rows are purged after a TTL.
CREATE TABLE IF NOT EXISTS drawer_add_outbox (
    idempotency_key TEXT NOT NULL PRIMARY KEY
  , branch_doc_id BLOB NOT NULL
  , entry BLOB NOT NULL
  , staged_branch_heads BLOB NOT NULL
  , state TEXT NOT NULL CHECK (state IN ('pending-add', 'committed-staged', 'done'))
  , temporary INTEGER NOT NULL
  , created_at INTEGER NOT NULL
  , done_at INTEGER NULL
) STRICT;
