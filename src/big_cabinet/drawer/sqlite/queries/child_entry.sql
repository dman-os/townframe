-- name: child_entry :optional
-- param: parent_id String
-- param: child_id String
-- returns: access String
SELECT access
  FROM big_cabinet_drawer_children
 WHERE parent_id = :parent_id
   AND child_id = :child_id;
