-- name: ancestors :many
-- param: document bool
-- param: target_id String
-- returns: drawer_id String, ordinary Option<i64>, public Option<i64>, sensitive Option<i64>
WITH RECURSIVE levels(access, level) AS (
    VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
), reached(drawer_id, level, audience) AS (
    SELECT drawer_id
         , 3
         , CASE WHEN :document AND json_extract(descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
                THEN 'public' ELSE 'ordinary' END
      FROM big_cabinet_drawers
     WHERE drawer_id = :target_id
    UNION
    SELECT docs.drawer_id
         , levels.level
         , 'ordinary'
      FROM big_cabinet_drawer_docs AS docs
      JOIN levels ON levels.access = docs.access
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = docs.drawer_id
     WHERE :document AND docs.doc_id = :target_id
       AND json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION
    SELECT meta.drawer_id
         , 3
         , meta.root
      FROM big_cabinet_drawer_meta_docs AS meta
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = meta.drawer_id
     WHERE :document AND meta.doc_id = :target_id
       AND meta.root IN ('public', 'sensitive')
       AND json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION
    SELECT drawer_id
         , 3
         , 'sensitive'
      FROM big_cabinet_drawers
     WHERE :document AND json_extract(descriptor_json, '$.layout.sensitiveRoot') = :target_id
       AND json_extract(descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION
    SELECT links.parent_id
         , MIN(reached.level, levels.level)
         , reached.audience
      FROM reached
      JOIN big_cabinet_drawer_children AS links ON links.child_id = reached.drawer_id
      JOIN levels ON levels.access = links.access
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = links.parent_id
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
)
SELECT drawer_id
     , MAX(CASE WHEN audience = 'ordinary' THEN level END) AS ordinary
     , MAX(CASE WHEN audience = 'public' THEN level END) AS public
     , MAX(CASE WHEN audience = 'sensitive' THEN level END) AS sensitive
  FROM reached
 GROUP BY drawer_id
 ORDER BY drawer_id;
