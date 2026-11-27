-- name: doc_epoch :optional
-- param: doc_id Vec<u8>
-- returns: epoch i64
SELECT epoch
  FROM kh_doc_epoch
 WHERE doc_id = :doc_id
