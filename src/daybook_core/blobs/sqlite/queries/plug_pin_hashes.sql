-- name: plug_pin_hashes :many
-- param: plug_id String
-- returns: blob_hash String
SELECT blob_hash FROM blob_pin_plug_state WHERE plug_id = :plug_id
