-- name: cgka_op_index_of :many
-- param: doc_id Vec<u8>
-- returns: digest Vec<u8>, predecessor Option<Vec<u8>>
SELECT o.digest
     , p.predecessor
  FROM kh_cgka_op AS o
  LEFT JOIN kh_cgka_op_pred AS p
    ON p.doc_id = o.doc_id
   AND p.digest = o.digest
 WHERE o.doc_id = :doc_id
