-- name: part_latest_cursor :optional
-- param: part_key Vec<u8>
-- returns: latest_cursor i64
SELECT latest_cursor
  FROM big_automata_parts
 WHERE part_key = :part_key
