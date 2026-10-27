-- name: ciphertext_backfill_rows :many
-- param: scope_id i64
-- returns: sedimentree_id Vec<u8>, content_ref Vec<u8>, digest Vec<u8>, kind i64, blob Vec<u8>
SELECT sedimentree_id
     , commit_id AS content_ref
     , digest
     , 0 AS kind
     , blob
  FROM big_repo_subduction_commits WHERE scope_id = :scope_id
 UNION ALL
SELECT sedimentree_id
     , head_id AS content_ref
     , digest
     , 1 AS kind
     , blob
  FROM big_repo_subduction_fragments WHERE scope_id = :scope_id;
