-- name: dek_rewrap :exec
-- param: nonce Vec<u8>
-- param: wrapped_dek Vec<u8>
-- param: kek_version i64
-- param: dek_id String
-- param: dek_version i64
UPDATE kh_dek
   SET nonce = :nonce
     , wrapped_dek = :wrapped_dek
     , kek_version = :kek_version
 WHERE dek_id = :dek_id
   AND dek_version = :dek_version
