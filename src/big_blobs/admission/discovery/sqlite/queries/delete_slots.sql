-- name: delete_slots :exec
-- param: drawer_id String
DELETE FROM big_blobs_discovery_slots WHERE drawer_id = :drawer_id;
