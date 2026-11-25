-- name: okv_floor :one
-- param: namespace_id i64
-- returns: floor Option<i64>
SELECT MIN(cursor) AS floor
  FROM big_automata_okv_consumers
 WHERE namespace_id = :namespace_id
