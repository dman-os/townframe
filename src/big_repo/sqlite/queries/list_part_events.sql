-- name: list_part_events :many
-- param: scope_id i64
-- param: part_id Vec<u8>
-- param: cursor i64
-- param: removed i64
-- param: limit i64
-- returns: obj_id Vec<u8>, payload_json Option<String>, event_type i64, txid i64
WITH cutoff AS (
    SELECT MAX(txid) AS txid
      FROM (
          SELECT m.txid FROM big_sync_members m
           WHERE m.scope_id = :scope_id
             AND m.maybe_part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id)
             AND m.txid > :cursor
             AND (m.event_type != :removed OR m.added_at <= :cursor)
           ORDER BY m.txid, m.obj_ref
           LIMIT :limit
      )
)
SELECT o.obj_id
     , o.payload_json
     , m.event_type
     , m.txid
  FROM big_sync_members m
  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
 WHERE m.scope_id = :scope_id
   AND m.maybe_part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id)
   AND m.txid > :cursor AND m.txid <= (SELECT txid FROM cutoff)
   AND (m.event_type != :removed OR m.added_at <= :cursor)
 ORDER BY m.txid, m.obj_ref;
