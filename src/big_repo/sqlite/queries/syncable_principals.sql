-- name: syncable_principals :many
-- param: scope_id i64
-- param: part_id Vec<u8>
-- returns: principal_id Vec<u8>
SELECT s.principal_id FROM big_sync_syncable s
  JOIN big_sync_parts p ON p.part_ref = s.part_ref
 WHERE s.scope_id = :scope_id AND p.part_id = :part_id
 ORDER BY s.principal_id;
