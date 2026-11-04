-- name: insert_retention_pin :exec
-- param: doc_id String
-- param: digest String
-- param: length_octets i64
INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets)
VALUES (:doc_id, :digest, NULL, :length_octets)
ON CONFLICT (doc_id, digest) DO NOTHING;
