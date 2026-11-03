-- name: insert_parked :exec
-- param: doc_id String
-- param: datum_key String
-- param: source_revision i64
-- param: membership Vec<u8>
INSERT INTO big_blobs_projection_parked
       (doc_id, datum_key, source_revision, membership)
VALUES (:doc_id, :datum_key, :source_revision, :membership)
ON CONFLICT (doc_id, datum_key) DO UPDATE SET
       source_revision = excluded.source_revision
     , membership = excluded.membership
 WHERE big_blobs_projection_parked.source_revision < excluded.source_revision;
