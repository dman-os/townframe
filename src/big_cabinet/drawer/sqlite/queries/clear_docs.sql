-- name: clear_docs :exec
-- param: drawer_id String
DELETE FROM big_cabinet_drawer_docs
 WHERE drawer_id = :drawer_id;
