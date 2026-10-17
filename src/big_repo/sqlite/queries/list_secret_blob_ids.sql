-- name: list_secret_blob_ids :many
-- param: scope_id i64
-- param: kind i64
-- returns: blob_id Vec<u8>
SELECT blob_id
  FROM big_repo_secret_blobs
 WHERE scope_id = :scope_id AND kind = :kind
 ORDER BY blob_id;
