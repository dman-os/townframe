-- name: walker_state_value :optional
-- param: namespace String
-- param: consumer_id String
-- param: state_key Vec<u8>
-- returns: state_value Vec<u8>
SELECT state_value
  FROM delta_walker_key_state
 WHERE namespace = :namespace AND consumer_id = :consumer_id AND state_key = :state_key
