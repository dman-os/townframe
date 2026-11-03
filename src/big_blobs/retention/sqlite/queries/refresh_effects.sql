-- name: refresh_effects :exec
INSERT OR IGNORE INTO big_blobs_retention_effects (digest)
SELECT digest FROM big_blobs_retention
UNION SELECT digest FROM big_blobs_writer_claims
UNION SELECT digest FROM big_blobs_writer_effects;
