-- name: list_dispatches :many
-- returns: id String, payload_json String
SELECT id, payload_json FROM dispatches ORDER BY id
