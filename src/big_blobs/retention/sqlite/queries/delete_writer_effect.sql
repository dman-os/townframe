-- name: delete_writer_effect :exec
-- param: inventory_id String
-- param: digest String
DELETE FROM big_blobs_writer_effects
 WHERE inventory_id = :inventory_id AND digest = :digest;
