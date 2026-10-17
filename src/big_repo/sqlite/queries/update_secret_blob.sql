-- name: update_secret_blob :exec
-- param: dek_id String
-- param: dek_version i64
-- param: ciphertext Vec<u8>
-- param: nonce Vec<u8>
-- param: scope_id i64
-- param: kind i64
-- param: blob_id Vec<u8>
UPDATE big_repo_secret_blobs
   SET dek_id = :dek_id
     , dek_version = :dek_version
     , ciphertext = :ciphertext
     , nonce = :nonce
 WHERE scope_id = :scope_id AND kind = :kind AND blob_id = :blob_id;
