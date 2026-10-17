-- name: insert_commit :exec
-- param: scope_id i64
-- param: sedimentree_id Vec<u8>
-- param: commit_id Vec<u8>
-- param: digest Vec<u8>
-- param: signed Vec<u8>
-- param: blob Vec<u8>
INSERT OR IGNORE INTO big_repo_subduction_commits (scope_id, sedimentree_id, commit_id, digest, signed, blob)
VALUES (:scope_id, :sedimentree_id, :commit_id, :digest, :signed, :blob);
