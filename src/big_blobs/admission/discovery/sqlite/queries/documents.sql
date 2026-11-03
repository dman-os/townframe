-- name: documents :many
-- returns: drawer_id String
SELECT DISTINCT drawer_id
  FROM big_blobs_discovery_reach
 ORDER BY drawer_id;
