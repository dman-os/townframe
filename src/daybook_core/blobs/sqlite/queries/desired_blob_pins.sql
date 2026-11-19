-- name: desired_blob_pins :many
-- returns: blob_hash String, length_octets i64
SELECT blob_hash, MAX(length_octets) AS length_octets
  FROM blob_pin_doc_state
 GROUP BY blob_hash
