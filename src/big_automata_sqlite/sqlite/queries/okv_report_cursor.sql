-- name: okv_report_cursor :exec
-- param: cursor i64
-- param: namespace_id i64
-- param: consumer String
UPDATE big_automata_okv_consumers
   SET cursor = MAX(cursor, :cursor)
 WHERE namespace_id = :namespace_id
   AND consumer = :consumer
