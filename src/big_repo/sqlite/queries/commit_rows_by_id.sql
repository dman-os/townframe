-- name: commit_rows_by_id :many
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: commit_id Vec<u8>
-- returns: signed Vec<u8>, blob Vec<u8>
-- into: crate::store::sqlite::sedimentree::SignedBlobRow
SELECT signed
     , blob
  FROM big_repo_subduction_commits
 WHERE scope_id = :scope_id AND sedimentree_id = :sedimentree_id AND commit_id = :commit_id
 ORDER BY digest;
