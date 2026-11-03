-- name: remove_roots :exec
-- param: drawer_id String
-- param: roots String
UPDATE big_blobs_discovery_roots
   SET authoritative = 0
 WHERE drawer_id = :drawer_id
   AND authoritative = 1
   AND meta_root NOT IN (SELECT value FROM json_each(:roots));
