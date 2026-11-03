-- name: insert_upload_receipt :exec
-- param: owner String
-- param: digest String
INSERT INTO big_blobs_upload_receipts (owner, digest)
VALUES (:owner, :digest);
