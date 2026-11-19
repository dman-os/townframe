-- name: release_uploads_by_digest :exec
-- param: digest String
UPDATE big_blobs_upload_receipts
   SET released = 1
 WHERE digest = :digest
   AND released = 0;
