-- name: insert_upload_receipt :exec
-- param: owner String
-- param: digest String
-- param: held_at i64
INSERT INTO big_blobs_upload_receipts (owner, digest, held_at)
VALUES (:owner, :digest, :held_at);
