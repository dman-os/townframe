-- name: clear_meta_docs :exec
-- param: drawer_id String
DELETE FROM big_cabinet_drawer_meta_docs
 WHERE drawer_id = :drawer_id;
