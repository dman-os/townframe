-- name: insert_owner :exec
-- param: owner String
-- param: digest String
INSERT OR IGNORE INTO big_blobs_byte_owners (owner, digest)
VALUES (:owner, :digest);
