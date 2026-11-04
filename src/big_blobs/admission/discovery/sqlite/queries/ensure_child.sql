-- name: ensure_child :exec
-- param: drawer_id String
INSERT OR IGNORE INTO big_blobs_discovery_documents (drawer_id) VALUES (:drawer_id);
