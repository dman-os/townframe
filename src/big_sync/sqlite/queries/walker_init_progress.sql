-- name: walker_init_progress :exec
-- param: namespace String
-- param: consumer_id String
INSERT OR IGNORE INTO delta_walker_progress(namespace, consumer_id, upstream_revision)
VALUES (:namespace, :consumer_id, 0)
