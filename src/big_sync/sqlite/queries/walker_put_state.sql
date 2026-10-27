-- name: walker_put_state :exec
-- param: namespace String
-- param: consumer_id String
-- param: state_key Vec<u8>
-- param: state_value Vec<u8>
INSERT INTO delta_walker_key_state(namespace, consumer_id, state_key, state_value)
VALUES (:namespace, :consumer_id, :state_key, :state_value)
ON CONFLICT(namespace, consumer_id, state_key) DO UPDATE SET state_value = excluded.state_value
