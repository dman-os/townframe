CREATE TABLE IF NOT EXISTS dispatches (
    id TEXT PRIMARY KEY NOT NULL
  , status TEXT NOT NULL
  , payload_json TEXT NOT NULL
  , wflow_job_id TEXT
  , updated_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_dispatches_wflow_job_id
    ON dispatches(wflow_job_id);
CREATE TABLE IF NOT EXISTS dispatch_cancelled_marks (
    dispatch_id TEXT PRIMARY KEY NOT NULL
  , created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS wflow_partition_frontier (
    wflow_partition_id TEXT PRIMARY KEY NOT NULL
  , frontier INTEGER NOT NULL
  , updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS init_per_node (
    init_id TEXT PRIMARY KEY
  , completed_at TEXT NOT NULL
) STRICT;
