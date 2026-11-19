-- name: delete_plug_pins :exec
-- param: plug_id String
DELETE FROM blob_pin_plug_state WHERE plug_id = :plug_id
