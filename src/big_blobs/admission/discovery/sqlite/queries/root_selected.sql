-- name: root_selected :many
-- param: root_id String
-- returns: selected i64
SELECT 1 AS selected
  FROM big_blobs_discovery_policies
 WHERE drawer_id = :root_id AND policy = 'root';
