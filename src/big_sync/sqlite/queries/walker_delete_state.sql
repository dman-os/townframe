-- name: walker_delete_state :exec
-- param: namespace String
-- param: consumer_id String
-- param: state_key Vec<u8>
DELETE FROM delta_walker_key_state
 WHERE namespace = :namespace AND consumer_id = :consumer_id AND state_key = :state_key
