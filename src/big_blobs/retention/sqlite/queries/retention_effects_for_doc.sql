-- name: retention_effects_for_doc :exec
-- param: doc_id String
INSERT OR IGNORE INTO big_blobs_retention_effects (digest)
SELECT digest FROM big_blobs_retention WHERE doc_id = :doc_id;
