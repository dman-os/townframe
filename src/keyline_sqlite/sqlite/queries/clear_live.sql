-- name: clear_live :exec
DELETE FROM keyline_live
 WHERE s IN (SELECT s FROM keyline_root)
