-- name: docs_for_tag :many
-- param: facet_tag String
-- returns: document_id String, branch_id String, branch_heads_json String
SELECT document_id, branch_id, branch_heads_json
  FROM facet_set_doc_facets
 WHERE facet_tag = :facet_tag
 GROUP BY document_id, branch_id, branch_heads_json
 ORDER BY document_id ASC, branch_id ASC
