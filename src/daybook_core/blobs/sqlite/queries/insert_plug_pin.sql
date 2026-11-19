-- name: insert_plug_pin :exec
-- param: plug_id String
-- param: blob_hash String
-- param: length_octets i64
INSERT INTO blob_pin_plug_state (plug_id, blob_hash, length_octets)
VALUES (:plug_id, :blob_hash, :length_octets)
