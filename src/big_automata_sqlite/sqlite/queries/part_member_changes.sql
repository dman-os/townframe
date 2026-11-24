-- name: part_member_changes :one
-- param: part_key Vec<u8>
-- param: since i64
-- returns: count i64
SELECT COUNT(*) AS count
  FROM big_automata_members
 WHERE part_key = :part_key
   AND rev > :since
