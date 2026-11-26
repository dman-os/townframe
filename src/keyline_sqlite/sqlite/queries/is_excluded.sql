-- name: is_excluded :one
-- param: ctx i64
-- param: node Vec<u8>
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM keyline_excl
          WHERE ctx = :ctx
            AND node = :node
       ) AS found
