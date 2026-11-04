-- name: delete_applied_grant :exec
-- param: plug_id String
-- param: drawer_id String
DELETE FROM nodule_applied_grants
 WHERE plug_id = :plug_id
   AND drawer_id = :drawer_id;
