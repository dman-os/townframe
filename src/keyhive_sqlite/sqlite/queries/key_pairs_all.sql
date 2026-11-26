-- name: key_pairs_all :many
-- returns: public_key Vec<u8>, sealed Vec<u8>, nonce Vec<u8>, dek_id String, dek_version i64
SELECT public_key
     , sealed
     , nonce
     , dek_id
     , dek_version
  FROM kh_key_pair
