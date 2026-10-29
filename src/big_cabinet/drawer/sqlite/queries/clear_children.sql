-- name: clear_children :exec
-- param: parent_id String
DELETE FROM big_cabinet_drawer_children
 WHERE parent_id = :parent_id;
