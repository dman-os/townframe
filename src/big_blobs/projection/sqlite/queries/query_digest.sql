-- name: query_digest :many
-- param: blob_id Vec<u8>
-- returns: doc_id String, datum_key String
SELECT doc_id
     , datum_key
  FROM big_blobs_projection_routes
 WHERE blob_id = :blob_id
 ORDER BY doc_id, datum_key;
