-- name: upsert_local_branch :exec
-- param: doc_id String
-- param: branch_path String
-- param: branch_doc_id Vec<u8>
-- param: vtag_version String
-- param: vtag_actor_id String
-- param: updated_at i64
INSERT INTO drawer_local_branches (
    doc_id
  , branch_path
  , branch_doc_id
  , vtag_version
  , vtag_actor_id
  , updated_at
) VALUES (:doc_id, :branch_path, :branch_doc_id, :vtag_version, :vtag_actor_id, :updated_at)
ON CONFLICT(doc_id, branch_path) DO UPDATE SET
    branch_doc_id = excluded.branch_doc_id
  , vtag_version = excluded.vtag_version
  , vtag_actor_id = excluded.vtag_actor_id
  , updated_at = excluded.updated_at
