-- name: cursors :many
-- returns: facet_tag String, revision i64
SELECT facet_tag
     , revision
  FROM big_cabinet_facet_ref_progress;
