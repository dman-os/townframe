-- name: delete_old_slots :exec
-- param: drawer_id String
-- param: meta_root String
-- param: names String
DELETE FROM big_blobs_discovery_slots
 WHERE drawer_id = :drawer_id
   AND meta_root = :meta_root
   AND name NOT IN (SELECT value FROM json_each(:names));
