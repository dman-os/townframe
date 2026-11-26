-- name: fragment_heads :many
-- param: tree_id Vec<u8>
-- returns: fragment_head Vec<u8>
SELECT fragment_head
  FROM sedimentree_fragments
 WHERE tree_id = :tree_id
