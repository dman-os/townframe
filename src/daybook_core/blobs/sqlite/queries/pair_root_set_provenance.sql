-- name: pair_root_set_provenance :exec
-- param: doc_id String
-- param: branch_path String
-- param: cipher_hash String
UPDATE blob_pair_root
   SET doc_id = :doc_id
     , branch_path = :branch_path
 WHERE cipher_hash = :cipher_hash
