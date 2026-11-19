-- name: docs_for_branch_id :many
-- param: branch_id String
-- returns: document_id String, branch_id String, facet_tag String, branch_heads_json String
SELECT document_id, branch_id, facet_tag, branch_heads_json
  FROM facet_set_doc_facets
 WHERE branch_id = :branch_id
 GROUP BY document_id, branch_id, facet_tag, branch_heads_json
 ORDER BY document_id ASC, branch_id ASC
