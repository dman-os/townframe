-- name: list_local_branches :many
-- param: doc_id String
-- returns: branch_path String, branch_doc_id Vec<u8>
SELECT branch_path
     , branch_doc_id
  FROM drawer_local_branches
 WHERE doc_id = :doc_id
 ORDER BY branch_path ASC
