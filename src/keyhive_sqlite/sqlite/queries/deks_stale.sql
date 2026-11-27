-- name: deks_stale :many
-- param: kek_version i64
-- returns: dek_id String, dek_version i64, algorithm String, nonce Vec<u8>, wrapped_dek Vec<u8>, kek_version i64
SELECT dek_id
     , dek_version
     , algorithm
     , nonce
     , wrapped_dek
     , kek_version
  FROM kh_dek
 WHERE kek_version <> :kek_version
