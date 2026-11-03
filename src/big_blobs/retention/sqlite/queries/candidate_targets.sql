-- name: candidate_targets :many
-- param: doc_id String
-- param: inventory_kind String
-- returns: drawer_id String, root String, name String, doc_id String
WITH containing(drawer_id) AS (
       SELECT drawer_id FROM big_cabinet_drawer_docs WHERE doc_id = :doc_id
       UNION
       SELECT links.parent_id
         FROM big_cabinet_drawer_children AS links
         JOIN big_cabinet_drawers AS child ON child.drawer_id = links.child_id
        WHERE links.child_id = :doc_id
          AND json_extract(child.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
)
SELECT meta.drawer_id
     , meta.root
     , meta.name
     , meta.doc_id
  FROM containing
  JOIN big_cabinet_drawer_meta_docs AS meta USING (drawer_id)
 WHERE meta.kind = :inventory_kind
UNION
SELECT drawer_id
     , root
     , name
     , inventory_id
  FROM big_blobs_writer_targets
 WHERE doc_id = :doc_id;
