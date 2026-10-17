-- name: has_more_part_events :one
-- param: scope_id i64
-- param: part_id Vec<u8>
-- param: cursor i64
-- param: cutoff i64
-- param: removed i64
-- returns: has_more bool
SELECT EXISTS (
    SELECT 1 FROM big_sync_members m
     WHERE m.scope_id = :scope_id
       AND m.maybe_part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = :scope_id AND part_id = :part_id)
       AND m.txid > :cursor AND m.txid > :cutoff
       AND (m.event_type != :removed OR m.added_at <= :cursor)
) AS has_more;
