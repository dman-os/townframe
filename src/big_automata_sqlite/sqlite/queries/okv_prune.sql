-- name: okv_prune :exec
-- param: namespace_id i64
DELETE FROM big_automata_okv_entries
 WHERE namespace_id = :namespace_id
   AND value IS NULL
   AND rev <= (
        SELECT MIN(cursor)
          FROM big_automata_okv_consumers
         WHERE namespace_id = :namespace_id
   )
