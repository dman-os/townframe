-- name: put_drawer :exec
-- param: drawer_id String
-- param: descriptor_json String
INSERT INTO big_cabinet_drawers (drawer_id, descriptor_json)
VALUES (:drawer_id, :descriptor_json)
ON CONFLICT (drawer_id) DO UPDATE SET descriptor_json = excluded.descriptor_json;
