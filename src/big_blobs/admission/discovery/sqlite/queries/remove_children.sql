-- name: remove_children :exec
-- param: drawer_id String
-- param: children String
DELETE FROM big_blobs_discovery_edges
 WHERE parent_id = :drawer_id
   AND child_id NOT IN (SELECT value FROM json_each(:children));
