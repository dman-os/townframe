-- name: drawers :many
-- returns: drawer_id String, descriptor_json String
-- into: crate::drawer::sqlite::DrawerRow
SELECT drawer_id
     , descriptor_json
  FROM big_cabinet_drawers
 ORDER BY drawer_id;
