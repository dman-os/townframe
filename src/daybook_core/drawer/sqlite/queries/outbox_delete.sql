-- name: outbox_delete :exec
-- param: idempotency_key String
DELETE FROM drawer_add_outbox WHERE idempotency_key = :idempotency_key
