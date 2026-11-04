-- name: projected_revision :many
-- param: doc_id String
-- param: datum_key String
-- returns: source_revision i64
SELECT source_revision
  FROM big_blobs_projection_routes
 WHERE doc_id = :doc_id
   AND datum_key = :datum_key;
