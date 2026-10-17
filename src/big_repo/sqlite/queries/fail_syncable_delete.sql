-- name: fail_syncable_delete :exec
CREATE TRIGGER fail_syncable_delete BEFORE DELETE ON big_sync_syncable
BEGIN SELECT RAISE(ABORT, 'injected syncable failure'); END;
