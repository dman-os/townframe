-- name: changes_beyond :one
-- param: after i64
-- param: until i64
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM sedimentree_trees
          WHERE rev > :after
            AND rev <= :until
       ) AS found
