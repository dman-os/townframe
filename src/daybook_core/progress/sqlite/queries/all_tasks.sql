-- name: all_tasks :many
-- returns: id String, title Option<String>, created_at_unix_secs i64, updated_at_unix_secs i64, viewed_at_unix_secs Option<i64>, dismissed_at_unix_secs Option<i64>, state_json String, retention_json String, retention_override_json Option<String>, latest_update_sequence Option<i64>, latest_update_at_unix_secs Option<i64>, latest_update_json Option<String>
SELECT id
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
  FROM progress_tasks
