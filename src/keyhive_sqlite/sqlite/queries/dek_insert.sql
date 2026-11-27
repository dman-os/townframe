-- name: dek_insert :exec
-- param: dek_id String
-- param: dek_version i64
-- param: algorithm String
-- param: nonce Vec<u8>
-- param: wrapped_dek Vec<u8>
-- param: kek_version i64
INSERT INTO kh_dek
     ( dek_id
     , dek_version
     , algorithm
     , nonce
     , wrapped_dek
     , kek_version
     )
VALUES (:dek_id, :dek_version, :algorithm, :nonce, :wrapped_dek, :kek_version)
