-- name: clear_dispatch_cancelled :exec
-- param: dispatch_id String
DELETE FROM dispatch_cancelled_marks WHERE dispatch_id = :dispatch_id
