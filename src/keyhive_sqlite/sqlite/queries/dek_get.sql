-- name: dek_get :optional
-- param: dek_id String
-- param: dek_version i64
-- returns: algorithm String, nonce Vec<u8>, wrapped_dek Vec<u8>, kek_version i64
SELECT algorithm
     , nonce
     , wrapped_dek
     , kek_version
  FROM kh_dek
 WHERE dek_id = :dek_id
   AND dek_version = :dek_version
