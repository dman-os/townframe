-- name: drawers_for_roots :many
-- param: roots String
-- returns: drawer_id String
SELECT drawer_id
  FROM big_cabinet_drawers
 WHERE json_extract(descriptor_json, '$.layout.sensitiveRoot') IN (SELECT value FROM json_each(:roots));
