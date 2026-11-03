-- name: query_doc :many
-- param: doc_id String
-- returns: doc_id String, datum_key String, blob_id Vec<u8>, length_octets String
SELECT doc_id
     , datum_key
     , blob_id
     , length_octets
  FROM big_blobs_projection_routes
 WHERE blob_id IS NOT NULL
   AND doc_id = :doc_id
 ORDER BY datum_key;
