-- name: list_cancelled_dispatches :many
-- returns: dispatch_id String
SELECT dispatch_id FROM dispatch_cancelled_marks
