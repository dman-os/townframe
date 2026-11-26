-- name: dek_latest :optional
-- param: dek_id String
-- returns: dek_version i64, algorithm String, nonce Vec<u8>, wrapped_dek Vec<u8>, kek_version i64
SELECT dek_version
     , algorithm
     , nonce
     , wrapped_dek
     , kek_version
  FROM kh_dek
 WHERE dek_id = :dek_id
 ORDER BY dek_version DESC
 LIMIT 1
