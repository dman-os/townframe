-- name: delete_policy :exec
-- param: drawer_id String
-- param: policy String
DELETE FROM big_blobs_discovery_policies
 WHERE drawer_id = :drawer_id AND policy = :policy;
