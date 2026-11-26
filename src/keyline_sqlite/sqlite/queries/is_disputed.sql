-- name: is_disputed :one
-- param: subject Vec<u8>
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM keyline_disputed
          WHERE subject = :subject
       ) AS found
