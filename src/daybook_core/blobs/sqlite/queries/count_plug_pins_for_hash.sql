-- name: count_plug_pins_for_hash :one
-- param: blob_hash String
-- returns: count i64
SELECT COUNT(*) AS count FROM blob_pin_plug_state WHERE blob_hash = :blob_hash
