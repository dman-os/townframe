-- name: cgka_op_counts :many
-- returns: doc_id Vec<u8>, ops i64
-- One ordered pass over the covering unique index, whose leading column is the group: no sort, no
-- temporary table, and no operation bytes read. Documents without operations have no row to
-- count.
SELECT doc_id
     , count(*) AS ops
  FROM kh_cgka_op
 GROUP BY doc_id
