-- name: drawer_descriptor :many
-- param: drawer_id String
-- returns: descriptor_json String
SELECT descriptor_json
  FROM big_cabinet_drawers
 WHERE drawer_id = :drawer_id;
