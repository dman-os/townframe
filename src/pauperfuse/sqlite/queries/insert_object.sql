-- name: insert_object :exec
-- param: doc_id String
-- param: relative_path String
-- param: provider_hash Option<String>
-- param: backend_hash Option<String>
INSERT INTO pauperfuse_object_state (doc_id, relative_path, provider_hash, backend_hash)
VALUES (:doc_id, :relative_path, :provider_hash, :backend_hash);
