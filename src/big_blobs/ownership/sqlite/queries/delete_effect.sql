-- name: delete_effect :exec
-- param: digest String
DELETE FROM big_blobs_byte_effects WHERE digest = :digest;
