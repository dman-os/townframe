-- name: enqueue :exec
-- param: doc_id String
INSERT INTO big_blobs_work (doc_id, generation, dirty, waiting)
VALUES (:doc_id, 1, 1, 0)
ON CONFLICT (doc_id) DO UPDATE SET generation = generation + 1, dirty = 1;
