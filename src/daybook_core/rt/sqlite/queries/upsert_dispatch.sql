-- name: upsert_dispatch :exec
-- param: id String
-- param: status String
-- param: payload_json String
-- param: wflow_job_id Option<String>
INSERT INTO dispatches(id, status, payload_json, wflow_job_id, updated_at)
VALUES (:id, :status, :payload_json, :wflow_job_id, unixepoch())
ON CONFLICT(id) DO UPDATE SET
    status = excluded.status
  , payload_json = excluded.payload_json
  , wflow_job_id = excluded.wflow_job_id
  , updated_at = excluded.updated_at
