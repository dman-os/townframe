-- name: ref_tag_docs :many
-- param: tags String
-- returns: doc_id String
SELECT DISTINCT doc_id
  FROM big_cabinet_datum_set_entries
 WHERE datum_tag IN (SELECT value FROM json_each(:tags));
