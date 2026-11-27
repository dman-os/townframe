-- name: closure_certificates :many
-- param: s Vec<u8>
-- returns: kind i64, digest Vec<u8>, signed Vec<u8>
-- The closure of a subject: every delegation about it or about a node that stands over it, and
-- every revocation naming one of those.
SELECT kind
     , digest
     , signed
  FROM (
         SELECT d.kind AS kind
              , d.digest AS digest
              , d.signed AS signed
           FROM keyline_certificates d
          WHERE d.subject IN (SELECT :s AS node
                               UNION
                              SELECT n FROM keyline_reach WHERE s = :s AND lvl = 0)
          UNION ALL
         SELECT r.kind
              , r.digest
              , r.signed
           FROM keyline_certificates r
          WHERE r.revoke IN (SELECT d.digest
                               FROM keyline_certificates d
                              WHERE d.subject IN (SELECT :s AS node
                                                   UNION
                                                  SELECT n FROM keyline_reach WHERE s = :s AND lvl = 0))
       )
 ORDER BY kind, digest
