-- name: delete_secret_blob :exec
-- param: scope_id i64
-- param: kind i64
-- param: blob_id Vec<u8>
DELETE FROM big_repo_secret_blobs
 WHERE scope_id = :scope_id AND kind = :kind AND blob_id = :blob_id;
