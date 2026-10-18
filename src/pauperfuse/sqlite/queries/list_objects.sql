-- name: list_objects :many
-- returns: doc_id String, relative_path String, provider_hash Option<String>, backend_hash Option<String>
SELECT doc_id, relative_path, provider_hash, backend_hash FROM pauperfuse_object_state;
