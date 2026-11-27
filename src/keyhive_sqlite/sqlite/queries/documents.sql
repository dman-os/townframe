-- name: documents :many
-- returns: doc_id Vec<u8>
-- A loose index scan over the unique index, as `individuals`.
WITH RECURSIVE ids(doc_id) AS (
    SELECT min(doc_id) FROM kh_cgka_op
    UNION ALL
    SELECT (SELECT min(doc_id) FROM kh_cgka_op WHERE doc_id > ids.doc_id)
      FROM ids
     WHERE ids.doc_id IS NOT NULL
)
SELECT doc_id
  FROM ids
 WHERE doc_id IS NOT NULL
