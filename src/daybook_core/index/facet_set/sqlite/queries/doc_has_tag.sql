-- name: doc_has_tag :optional
-- param: document_id String
-- param: facet_tag String
-- returns: present i64
SELECT 1 AS present
  FROM facet_set_doc_facets
 WHERE document_id = :document_id
   AND facet_tag = :facet_tag
 LIMIT 1
