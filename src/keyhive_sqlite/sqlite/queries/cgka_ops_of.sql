-- name: cgka_ops_of :many
-- param: doc_id Vec<u8>
-- returns: digest Vec<u8>, signed Vec<u8>
SELECT digest
     , signed
  FROM kh_cgka_op
 WHERE doc_id = :doc_id
