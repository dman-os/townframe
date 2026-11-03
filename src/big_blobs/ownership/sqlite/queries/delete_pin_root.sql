-- name: delete_pin_root :exec
-- param: digest String
DELETE FROM big_blobs_byte_pin_roots WHERE digest = :digest;
