-- name: member_changes :one
-- param: scope_id i64
-- param: part_ref i64
-- param: since i64
-- returns: count i64
SELECT COUNT(*) AS count
  FROM big_sync_members
 WHERE scope_id = :scope_id
   AND maybe_part_ref = :part_ref
   AND txid > :since
