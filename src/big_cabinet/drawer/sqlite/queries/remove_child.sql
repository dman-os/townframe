-- name: remove_child :exec
-- param: parent_id String
-- param: child_id String
DELETE FROM big_cabinet_drawer_children
 WHERE parent_id = :parent_id
   AND child_id = :child_id;
