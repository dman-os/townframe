-- name: prekey_op_counts :many
-- returns: individual_id Vec<u8>, ops i64
-- One ordered pass over the primary key, whose leading column is the group: no sort, no
-- temporary table. Individuals without operations have no row to count.
SELECT individual_id
     , count(*) AS ops
  FROM kh_prekey_op
 GROUP BY individual_id
