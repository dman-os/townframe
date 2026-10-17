-- name: init_bucket_depth :exec
-- param: bucket_depth i64
INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES ('bucket_depth', :bucket_depth)
