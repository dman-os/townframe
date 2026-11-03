-- name: settle :exec
-- param: doc_id String
-- param: datum_key String
-- param: source_revision i64
-- param: blob_id Option<Vec<u8>>
-- param: length_octets Option<String>
INSERT INTO big_blobs_projection_routes
       (doc_id, datum_key, source_revision, blob_id, length_octets)
VALUES (:doc_id, :datum_key, :source_revision, :blob_id, :length_octets)
ON CONFLICT (doc_id, datum_key) DO UPDATE SET
       source_revision = excluded.source_revision
     , blob_id = excluded.blob_id
     , length_octets = excluded.length_octets;
