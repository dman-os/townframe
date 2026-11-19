-- name: desired_cipher_pins :many
-- returns: cipher_hash String, length_octets i64
SELECT cipher_hash, MAX(length_octets) AS length_octets
  FROM blob_pin_cipher_doc_state
 GROUP BY cipher_hash
