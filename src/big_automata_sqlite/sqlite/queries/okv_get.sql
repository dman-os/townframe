-- name: okv_get :optional
-- param: namespace_id i64
-- param: key Vec<u8>
-- returns: value Option<Vec<u8>>
SELECT value
  FROM big_automata_okv_entries
 WHERE namespace_id = :namespace_id
   AND key = :key
