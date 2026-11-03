-- name: writer_documents :many
-- param: document Option<String>
-- returns: document String, inventory bool
WITH drawers AS (
       SELECT drawer_id
         FROM big_cabinet_drawer_docs
        WHERE doc_id = :document
       UNION
       SELECT links.parent_id
         FROM big_cabinet_drawer_children AS links
         JOIN big_cabinet_drawers AS child ON child.drawer_id = links.child_id
        WHERE links.child_id = :document
          AND json_extract(child.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
       UNION
       SELECT drawer_id
         FROM big_blobs_writer_targets
        WHERE :document IS NULL OR doc_id = :document
       UNION
       SELECT drawer_id
         FROM big_cabinet_drawers
        WHERE drawer_id = :document
           OR json_extract(descriptor_json, '$.layout.sensitiveRoot') = :document
)
SELECT inventory_id AS document
     , 1 AS inventory
  FROM big_blobs_writer_targets
 WHERE :document IS NULL OR doc_id = :document
 UNION
SELECT inventory_id AS document
     , 1 AS inventory
  FROM big_blobs_writer_effects
 WHERE :document IS NULL
 UNION
SELECT inventory_id AS document
     , 1 AS inventory
  FROM big_blobs_inventory_registry
 WHERE :document IS NULL OR inventory_id = :document
 UNION
SELECT drawer.drawer_id AS document
     , 0 AS inventory
  FROM drawers
  JOIN big_cabinet_drawers AS drawer USING (drawer_id)
 WHERE json_extract(drawer.descriptor_json, '$.layout.sensitiveRoot') IS NOT NULL
 UNION
SELECT json_extract(drawer.descriptor_json, '$.layout.sensitiveRoot') AS document
     , 0 AS inventory
  FROM drawers
  JOIN big_cabinet_drawers AS drawer USING (drawer_id)
 WHERE json_extract(drawer.descriptor_json, '$.layout.sensitiveRoot') IS NOT NULL;
