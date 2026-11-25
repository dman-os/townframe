-- name: set_meta_generation :exec
-- param: generation Vec<u8>
UPDATE big_automata_meta
   SET generation = :generation
 WHERE id = 1
