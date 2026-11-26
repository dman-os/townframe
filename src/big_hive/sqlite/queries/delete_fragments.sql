-- name: delete_fragments :exec
-- param: tree_id Vec<u8>
DELETE FROM sedimentree_fragments
 WHERE tree_id = :tree_id
