-- name: fail_payload_update :exec
CREATE TRIGGER fail_big_repo_payload_update
BEFORE UPDATE OF payload_json ON big_sync_objs
WHEN hex(NEW.obj_id) = '0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E0E'
BEGIN SELECT RAISE(ABORT, 'injected payload failure'); END;
