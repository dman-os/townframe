-- name: find_scope_id :optional
-- param: scope_key String
-- returns: scope_id i64
SELECT scope_id FROM big_sync_scopes WHERE scope_key = :scope_key
