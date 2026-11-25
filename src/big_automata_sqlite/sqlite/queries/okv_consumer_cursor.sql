-- name: okv_consumer_cursor :optional
-- param: namespace_id i64
-- param: consumer String
-- returns: cursor i64
SELECT cursor
  FROM big_automata_okv_consumers
 WHERE namespace_id = :namespace_id
   AND consumer = :consumer
