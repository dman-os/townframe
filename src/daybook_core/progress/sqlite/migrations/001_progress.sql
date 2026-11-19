CREATE TABLE IF NOT EXISTS progress_tasks (
    id TEXT PRIMARY KEY
  , title TEXT
  , created_at_unix_secs INTEGER NOT NULL
  , updated_at_unix_secs INTEGER NOT NULL
  , viewed_at_unix_secs INTEGER
  , dismissed_at_unix_secs INTEGER
  , state_json TEXT NOT NULL
  , retention_json TEXT NOT NULL
  , retention_override_json TEXT
  , latest_update_sequence INTEGER
  , latest_update_at_unix_secs INTEGER
  , latest_update_json TEXT
) STRICT;
CREATE TABLE IF NOT EXISTS progress_task_updates (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT
  , task_id TEXT NOT NULL
  , at_unix_secs INTEGER NOT NULL
  , update_json TEXT NOT NULL
  , FOREIGN KEY(task_id) REFERENCES progress_tasks(id) ON DELETE CASCADE
) STRICT;
CREATE TABLE IF NOT EXISTS progress_task_tags (
    task_id TEXT NOT NULL
  , tag_path TEXT NOT NULL
  , PRIMARY KEY(task_id, tag_path)
  , FOREIGN KEY(task_id) REFERENCES progress_tasks(id) ON DELETE CASCADE
) STRICT;
CREATE INDEX IF NOT EXISTS idx_progress_tasks_state ON progress_tasks(state_json);
CREATE INDEX IF NOT EXISTS idx_progress_tasks_updated ON progress_tasks(updated_at_unix_secs DESC);
CREATE INDEX IF NOT EXISTS idx_progress_task_tags_path ON progress_task_tags(tag_path);
CREATE INDEX IF NOT EXISTS idx_progress_task_updates_task_seq ON progress_task_updates(task_id, sequence DESC);
