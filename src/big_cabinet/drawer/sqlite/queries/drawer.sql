-- name: drawer :optional
-- param: drawer_id String
-- returns: drawer_id String, descriptor_json String
SELECT drawer_id
     , descriptor_json
  FROM big_cabinet_drawers
 WHERE drawer_id = :drawer_id;
