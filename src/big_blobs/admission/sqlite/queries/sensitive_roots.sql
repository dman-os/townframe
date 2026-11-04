-- name: sensitive_roots :many
-- param: drawers String
-- returns: root String
SELECT json_extract(descriptor_json, '$.layout.sensitiveRoot') AS root
  FROM big_cabinet_drawers
 WHERE drawer_id IN (SELECT value FROM json_each(:drawers))
   AND json_extract(descriptor_json, '$.layout.sensitiveRoot') IS NOT NULL;
