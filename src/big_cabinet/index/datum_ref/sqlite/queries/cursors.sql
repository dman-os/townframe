-- name: cursors :many
-- returns: datum_tag String, revision i64
SELECT datum_tag
     , revision
  FROM big_cabinet_datum_ref_progress;
