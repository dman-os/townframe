-- name: root_drawers :many
-- param: document_id String
-- returns: drawer_id String
SELECT drawer_id
  FROM big_cabinet_drawers
 WHERE drawer_id = :document_id
    OR json_extract(descriptor_json, '$.layout.sensitiveRoot') = :document_id;
