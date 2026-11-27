-- name: set_epoch :exec
-- param: epoch i64
UPDATE keyline_meta
   SET epoch = :epoch
 WHERE id = 1
