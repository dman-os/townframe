-- name: scoped_doc_access :many
-- param: roots String
-- returns: doc_id String, level i64
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
), entries(doc_id, level) AS (
    SELECT docs.doc_id
         , MIN(reachable.level, levels.level)
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
      CROSS JOIN big_cabinet_drawer_docs AS docs ON docs.drawer_id = reachable.drawer_id
      JOIN levels ON levels.access = docs.access
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION ALL
    SELECT reachable.drawer_id
         , reachable.level
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
)
SELECT doc_id
     , MAX(level) AS level
  FROM entries
 GROUP BY doc_id
 ORDER BY doc_id;
