-- name: superseded :many
-- param: doc_id String
-- param: datum_key String
-- returns: source_revision i64, parked i64
SELECT source_revision
     , 0 AS parked
  FROM big_blobs_projection_routes
 WHERE doc_id = :doc_id
   AND datum_key = :datum_key
UNION ALL
SELECT source_revision
     , 1 AS parked
  FROM big_blobs_projection_parked
 WHERE doc_id = :doc_id
   AND datum_key = :datum_key;
