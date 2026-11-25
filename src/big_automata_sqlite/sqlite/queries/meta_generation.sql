-- name: meta_generation :one
-- returns: generation Vec<u8>
SELECT generation
  FROM big_automata_meta
 WHERE id = 1
