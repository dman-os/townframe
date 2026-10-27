-- name: children_of :many
-- param: parent_id String
-- returns: child_id String, access String
SELECT child_id
     , access
  FROM big_cabinet_drawer_children
 WHERE parent_id = :parent_id
 ORDER BY child_id;
