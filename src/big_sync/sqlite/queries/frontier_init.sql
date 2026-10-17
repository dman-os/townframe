-- name: frontier_init :exec
-- param: frontier_id String
INSERT OR IGNORE INTO generic_frontier_meta (frontier_id, revision)
VALUES (:frontier_id, 0)
