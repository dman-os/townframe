-- name: unprojected :many
-- param: announced String
-- returns: drawer_id String
SELECT child_id AS drawer_id
  FROM big_cabinet_drawer_children
UNION
SELECT value
  FROM json_each(:announced)
EXCEPT
SELECT drawer_id
  FROM big_cabinet_drawers
 ORDER BY drawer_id;
