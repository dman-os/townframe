-- name: delete_fragment :exec
-- param: tree_id Vec<u8>
-- param: fragment_head Vec<u8>
DELETE FROM sedimentree_fragments
 WHERE tree_id = :tree_id
   AND fragment_head = :fragment_head
