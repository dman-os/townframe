-- name: outbox_set_state :exec
-- param: state String
-- param: done_at Option<i64>
-- param: idempotency_key String
UPDATE drawer_add_outbox SET state = :state, done_at = :done_at WHERE idempotency_key = :idempotency_key
