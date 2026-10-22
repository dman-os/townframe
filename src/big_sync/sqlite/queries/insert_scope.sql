-- name: insert_scope :exec
-- param: scope_key String
INSERT INTO big_sync_scopes(scope_key) VALUES (:scope_key)
