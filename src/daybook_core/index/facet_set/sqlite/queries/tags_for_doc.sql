-- name: tags_for_doc :many
-- param: document_id String
-- returns: facet_tag String
SELECT DISTINCT facet_tag
  FROM facet_set_doc_facets
 WHERE document_id = :document_id
 ORDER BY facet_tag ASC
