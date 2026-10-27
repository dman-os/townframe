-- name: fail_member_insert :exec
CREATE TRIGGER fail_member_insert BEFORE INSERT ON big_sync_members
BEGIN SELECT RAISE(ABORT, 'injected member insert failure'); END;
