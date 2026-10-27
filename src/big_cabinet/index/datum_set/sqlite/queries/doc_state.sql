-- name: doc_state :optional
-- param: doc_id String
-- returns: state Vec<u8>
SELECT state
  FROM big_cabinet_datum_set_docs
 WHERE doc_id = :doc_id;
