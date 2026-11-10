-- name: insert_retention_plain :exec
-- param: doc_id String
-- param: digest String
-- param: plaintext String
-- param: length_octets i64
-- param: root String
INSERT INTO big_blobs_retention (doc_id, digest, plaintext, length_octets, root)
VALUES (:doc_id, :digest, :plaintext, :length_octets, :root)
ON CONFLICT (doc_id, digest) DO UPDATE SET plaintext = excluded.plaintext, root = excluded.root;
