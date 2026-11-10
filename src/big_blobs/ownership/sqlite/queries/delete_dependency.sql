-- name: delete_dependency :exec
-- param: digest String
DELETE FROM big_blobs_byte_dependencies WHERE digest = :digest;
