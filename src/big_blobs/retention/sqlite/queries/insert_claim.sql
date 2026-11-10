-- name: insert_claim :exec
-- param: doc_id String
-- param: drawer_id String
-- param: root String
-- param: name String
-- param: inventory_id String
-- param: digest String
-- param: length_octets i64
INSERT INTO big_blobs_writer_claims
    (doc_id, drawer_id, root, name, inventory_id, digest, length_octets)
VALUES (:doc_id, :drawer_id, :root, :name, :inventory_id, :digest, :length_octets)
ON CONFLICT DO NOTHING;
