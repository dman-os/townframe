-- name: fragment_row :optional
-- param: tree_id Vec<u8>
-- param: fragment_head Vec<u8>
-- returns: signed Vec<u8>, blob Vec<u8>
SELECT signed
     , blob
  FROM sedimentree_fragments
 WHERE tree_id = :tree_id
   AND fragment_head = :fragment_head
