-- name: count_plug_pins :one
-- param: plug_id String
-- returns: count i64
SELECT COUNT(*) AS count FROM blob_pin_plug_state WHERE plug_id = :plug_id
