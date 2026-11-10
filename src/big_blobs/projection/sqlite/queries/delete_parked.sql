-- name: delete_parked :exec
-- param: doc_id String
-- param: datum_key String
DELETE FROM big_blobs_projection_parked
 WHERE doc_id = :doc_id
   AND datum_key = :datum_key;
