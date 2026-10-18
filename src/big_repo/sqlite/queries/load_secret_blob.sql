-- name: load_secret_blob :optional
-- param: scope_id i64
-- param: kind i64
-- param: blob_id Vec<u8>
-- returns: dek_id String, dek_version i64, ciphertext Vec<u8>, nonce Vec<u8>
SELECT dek_id
     , dek_version
     , ciphertext
     , nonce
  FROM big_repo_secret_blobs
 WHERE scope_id = :scope_id AND kind = :kind AND blob_id = :blob_id;
