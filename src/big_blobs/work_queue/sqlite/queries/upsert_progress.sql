-- name: upsert_progress :exec
-- param: source String
-- param: revision i64
INSERT INTO big_blobs_progress (source, revision)
VALUES (:source, :revision)
ON CONFLICT (source) DO UPDATE SET revision = excluded.revision;
