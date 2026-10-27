-- name: scoped_docs :many
-- param: drawer_id String
-- param: level i64
-- returns: doc_id String, level i64, via Option<String>
WITH RECURSIVE levels(access, level) AS (
    VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
), reachable(drawer_id, level) AS (
    SELECT :drawer_id
         , :level
    UNION
    SELECT links.child_id
         , MIN(reachable.level, levels.level)
      FROM reachable
      JOIN big_cabinet_drawer_children AS links ON links.parent_id = reachable.drawer_id
      JOIN levels ON levels.access = links.access
      JOIN big_cabinet_drawers AS parent ON parent.drawer_id = reachable.drawer_id
     WHERE json_extract(parent.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
), entries(doc_id, level, via) AS (
    SELECT docs.doc_id
         , MIN(reachable.level, levels.level)
         , CASE WHEN reachable.drawer_id = :drawer_id THEN NULL ELSE reachable.drawer_id END
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
      JOIN big_cabinet_drawer_docs AS docs ON docs.drawer_id = reachable.drawer_id
      JOIN levels ON levels.access = docs.access
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION ALL
    SELECT reachable.drawer_id
         , reachable.level
         , CASE WHEN reachable.drawer_id = :drawer_id THEN NULL ELSE reachable.drawer_id END
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
), ranked AS (
    SELECT doc_id
         , level
         , via
         , ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY level DESC, via IS NOT NULL, via) AS ordinal
      FROM entries
)
SELECT doc_id
     , level
     , via
  FROM ranked
 WHERE ordinal = 1
 ORDER BY doc_id;
