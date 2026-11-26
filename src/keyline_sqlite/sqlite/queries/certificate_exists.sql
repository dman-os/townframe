-- name: certificate_exists :one
-- param: kind i64
-- param: digest Vec<u8>
-- returns: found bool
SELECT EXISTS (
         SELECT 1
           FROM keyline_certificates
          WHERE kind = :kind
            AND digest = :digest
       ) AS found
