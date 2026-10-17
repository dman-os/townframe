-- name: fail_part_cursor_update :exec
CREATE TRIGGER fail_part_cursor_update BEFORE UPDATE OF latest_cursor ON big_sync_parts
BEGIN SELECT RAISE(ABORT, 'injected part cursor failure'); END;
