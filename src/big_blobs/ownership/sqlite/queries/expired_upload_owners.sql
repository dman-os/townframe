-- name: expired_upload_owners :many
-- param: before i64
-- returns: owner String
SELECT owner
  FROM big_blobs_upload_receipts
 WHERE released = 0
   AND held_at < :before
 ORDER BY held_at;
