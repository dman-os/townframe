-- name: delete_retention_effect :exec
-- param: digest String
DELETE FROM big_blobs_retention_effects WHERE digest = :digest;
