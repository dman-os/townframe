-- name: insert_secret_blob :exec
-- param: scope_id i64
-- param: kind i64
-- param: blob_id Vec<u8>
-- param: dek_id String
-- param: dek_version i64
-- param: ciphertext Vec<u8>
-- param: nonce Vec<u8>
INSERT INTO big_repo_secret_blobs (
       scope_id
     , kind
     , blob_id
     , dek_id
     , dek_version
     , ciphertext
     , nonce
)
VALUES (:scope_id, :kind, :blob_id, :dek_id, :dek_version, :ciphertext, :nonce)
ON CONFLICT(scope_id, kind, blob_id) DO NOTHING;
