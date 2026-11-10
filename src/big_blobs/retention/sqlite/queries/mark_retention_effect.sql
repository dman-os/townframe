-- name: mark_retention_effect :exec
-- param: digest String
INSERT OR IGNORE INTO big_blobs_retention_effects (digest) VALUES (:digest);
