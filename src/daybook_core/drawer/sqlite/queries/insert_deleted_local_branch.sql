-- name: insert_deleted_local_branch :exec
-- param: doc_id String
-- param: branch_path String
-- param: branch_doc_id Vec<u8>
-- param: branch_heads_json String
-- param: vtag_version String
-- param: vtag_actor_id String
-- param: deleted_at i64
INSERT INTO drawer_local_branches_deleted (
    doc_id
  , branch_path
  , branch_doc_id
  , branch_heads_json
  , vtag_version
  , vtag_actor_id
  , deleted_at
) VALUES (:doc_id, :branch_path, :branch_doc_id, :branch_heads_json, :vtag_version, :vtag_actor_id, :deleted_at)
