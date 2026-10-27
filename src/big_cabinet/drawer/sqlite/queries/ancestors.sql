-- name: ancestors :many
-- param: target_id String
-- param: document bool
-- returns: drawer_id String, level i64
WITH RECURSIVE levels(access, level) AS (
    VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
), reached(drawer_id, level) AS (
    SELECT drawer_id
         , 3
      FROM big_cabinet_drawers
     WHERE drawer_id = :target_id
    UNION
    SELECT docs.drawer_id
         , levels.level
      FROM big_cabinet_drawer_docs AS docs
      JOIN levels ON levels.access = docs.access
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = docs.drawer_id
     WHERE :document AND docs.doc_id = :target_id
       AND json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION
    SELECT links.parent_id
         , MIN(reached.level, levels.level)
      FROM reached
      JOIN big_cabinet_drawer_children AS links ON links.child_id = reached.drawer_id
      JOIN levels ON levels.access = links.access
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = links.parent_id
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
)
SELECT drawer_id
     , MAX(level) AS level
  FROM reached
 GROUP BY drawer_id
 ORDER BY drawer_id;
