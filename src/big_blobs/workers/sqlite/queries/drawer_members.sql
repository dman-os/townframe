-- name: drawer_members :many
-- param: drawer_id String
-- returns: document_id String
SELECT doc_id AS document_id
  FROM big_cabinet_drawer_docs
 WHERE drawer_id = :drawer_id
 UNION
SELECT links.child_id AS document_id
  FROM big_cabinet_drawer_children AS links
  JOIN big_cabinet_drawers AS child ON child.drawer_id = links.child_id
 WHERE links.parent_id = :drawer_id
   AND json_extract(child.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
 UNION
SELECT doc_id AS document_id
  FROM big_blobs_writer_targets
 WHERE drawer_id = :drawer_id;
