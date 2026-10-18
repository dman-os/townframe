-- name: upsert_task :exec
-- param: id String
-- param: title String
-- param: created_at_unix_secs i64
-- param: updated_at_unix_secs i64
-- param: viewed_at_unix_secs Option<i64>
-- param: dismissed_at_unix_secs Option<i64>
-- param: state_json String
-- param: retention_json String
-- param: retention_override_json Option<String>
-- param: latest_update_sequence Option<i64>
-- param: latest_update_at_unix_secs Option<i64>
-- param: latest_update_json Option<String>
INSERT INTO progress_tasks(
    id
  , title
  , created_at_unix_secs
  , updated_at_unix_secs
  , viewed_at_unix_secs
  , dismissed_at_unix_secs
  , state_json
  , retention_json
  , retention_override_json
  , latest_update_sequence
  , latest_update_at_unix_secs
  , latest_update_json
) VALUES (
    :id
  , :title
  , :created_at_unix_secs
  , :updated_at_unix_secs
  , :viewed_at_unix_secs
  , :dismissed_at_unix_secs
  , :state_json
  , :retention_json
  , :retention_override_json
  , :latest_update_sequence
  , :latest_update_at_unix_secs
  , :latest_update_json
)
ON CONFLICT(id) DO UPDATE SET
    title = excluded.title
  , updated_at_unix_secs = excluded.updated_at_unix_secs
  , viewed_at_unix_secs = excluded.viewed_at_unix_secs
  , dismissed_at_unix_secs = excluded.dismissed_at_unix_secs
  , state_json = excluded.state_json
  , retention_json = excluded.retention_json
  , retention_override_json = excluded.retention_override_json
  , latest_update_sequence = excluded.latest_update_sequence
  , latest_update_at_unix_secs = excluded.latest_update_at_unix_secs
  , latest_update_json = excluded.latest_update_json
