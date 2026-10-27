-- name: put_child :exec
-- param: parent_id String
-- param: child_id String
-- param: access String
INSERT INTO big_cabinet_drawer_children (parent_id, child_id, access)
VALUES (:parent_id, :child_id, :access)
ON CONFLICT (parent_id, child_id) DO UPDATE SET access = excluded.access;
