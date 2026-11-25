-- name: okv_unregister_consumer :exec
-- param: namespace_id i64
-- param: consumer String
DELETE FROM big_automata_okv_consumers
 WHERE namespace_id = :namespace_id
   AND consumer = :consumer
