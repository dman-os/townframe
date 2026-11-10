-- name: upsert_root :exec
-- param: drawer_id String
-- param: meta_root String
INSERT INTO big_blobs_discovery_roots (drawer_id, meta_root, authoritative)
VALUES (:drawer_id, :meta_root, 1)
ON CONFLICT (drawer_id, meta_root) DO UPDATE SET authoritative = 1
 WHERE authoritative != 1;
