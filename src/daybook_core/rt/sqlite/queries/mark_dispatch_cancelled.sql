-- name: mark_dispatch_cancelled :exec
-- param: dispatch_id String
INSERT OR IGNORE INTO dispatch_cancelled_marks(dispatch_id, created_at)
VALUES (:dispatch_id, unixepoch())
