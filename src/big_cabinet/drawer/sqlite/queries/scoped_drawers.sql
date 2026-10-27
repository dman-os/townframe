-- name: scoped_drawers :many
-- param: roots String
-- returns: drawer_id String, descriptor_json String, level i64
WITH RECURSIVE levels(access, level) AS (
    VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
), reachable(drawer_id, level) AS (
    SELECT json_extract(value, '$[0]')
         , json_extract(value, '$[1]')
      FROM json_each(:roots)
    UNION
    SELECT links.child_id
         , MIN(reachable.level, levels.level)
      FROM reachable
      JOIN big_cabinet_drawer_children AS links ON links.parent_id = reachable.drawer_id
      JOIN levels ON levels.access = links.access
      JOIN big_cabinet_drawers AS parent ON parent.drawer_id = reachable.drawer_id
     WHERE json_extract(parent.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
)
SELECT drawer.drawer_id
     , drawer.descriptor_json
     , MAX(reachable.level) AS level
  FROM reachable
  JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
 GROUP BY drawer.drawer_id
 ORDER BY drawer.drawer_id;
