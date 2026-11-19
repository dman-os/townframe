-- name: local_branch_doc_id :optional
-- param: doc_id String
-- param: branch_path String
-- returns: branch_doc_id Vec<u8>
SELECT branch_doc_id
  FROM drawer_local_branches
 WHERE doc_id = :doc_id AND branch_path = :branch_path
