-- name: frontier_upsert_entry :exec
-- param: frontier_id String
-- param: namespace Option<Vec<u8>>
-- param: key Vec<u8>
-- param: revision i64
-- param: value Option<Vec<u8>>
INSERT INTO generic_frontier_entries (frontier_id, namespace, key, revision, value)
VALUES (:frontier_id, :namespace, :key, :revision, :value)
ON CONFLICT(frontier_id, key) DO UPDATE
   SET namespace = excluded.namespace
     , revision = excluded.revision
     , value = excluded.value
