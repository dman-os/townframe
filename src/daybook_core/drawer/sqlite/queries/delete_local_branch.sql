-- name: delete_local_branch :exec
-- param: doc_id String
-- param: branch_path String
DELETE FROM drawer_local_branches WHERE doc_id = :doc_id AND branch_path = :branch_path
