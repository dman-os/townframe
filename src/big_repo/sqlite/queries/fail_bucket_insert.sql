-- name: fail_bucket_insert :exec
CREATE TRIGGER fail_bucket_insert BEFORE INSERT ON big_sync_buckets
BEGIN SELECT RAISE(ABORT, 'injected bucket failure'); END;
