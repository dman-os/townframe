-- name: release_staging :exec
-- param: owner String
UPDATE big_blobs_upload_receipts
   SET released = 1
 WHERE owner = :owner
   AND released = 0;
