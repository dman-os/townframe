-- name: delete_stale_reach :exec
DELETE FROM big_blobs_discovery_reach
 WHERE (root_id, drawer_id, meta_root, name) NOT IN (SELECT * FROM big_blobs_discovery_next_reach);
