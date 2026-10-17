-- name: frontier_entry_value :optional
-- param: frontier_id String
-- param: key Vec<u8>
-- returns: value Option<Vec<u8>>
SELECT value
  FROM generic_frontier_entries
 WHERE frontier_id = :frontier_id AND key = :key
