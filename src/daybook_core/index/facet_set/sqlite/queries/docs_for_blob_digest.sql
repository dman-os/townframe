-- name: docs_for_blob_digest :many
-- param: blob_facet_tag String
-- param: blob_digest String
-- returns: document_id String, branch_id String, branch_heads_json String
SELECT bf.document_id
     , bf.branch_id
     , ff.branch_heads_json
  FROM facet_set_doc_blob_facets AS bf
  JOIN facet_set_doc_facets AS ff
    ON ff.document_id = bf.document_id
   AND ff.branch_id = bf.branch_id
   AND ff.facet_tag = :blob_facet_tag
 WHERE bf.blob_digest = :blob_digest
 GROUP BY bf.document_id, bf.branch_id, ff.branch_heads_json
 ORDER BY bf.document_id ASC, bf.branch_id ASC
