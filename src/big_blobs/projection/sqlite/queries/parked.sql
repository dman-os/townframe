-- name: parked :many
-- param: before Option<i64>
-- returns: doc_id String, datum_key String, source_revision i64, membership Vec<u8>
SELECT doc_id
     , datum_key
     , source_revision
     , membership
  FROM big_blobs_projection_parked
 WHERE :before IS NULL OR source_revision < :before
 ORDER BY doc_id, datum_key;
