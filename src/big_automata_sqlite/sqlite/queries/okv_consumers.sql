-- name: okv_consumers :many
-- param: namespace_id i64
-- returns: consumer String, cursor i64
SELECT consumer
     , cursor
  FROM big_automata_okv_consumers
 WHERE namespace_id = :namespace_id
 ORDER BY consumer
