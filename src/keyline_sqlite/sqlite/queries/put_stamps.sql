-- name: put_stamps :exec
-- param: version i64
-- param: subject Vec<u8>
-- Stamp every root of the computation, and the queried subject, as computed at `version`.
INSERT INTO keyline_live_at (subject, version)
SELECT subject
     , :version
  FROM (
         SELECT s AS subject FROM keyline_root
          UNION
         SELECT :subject
       )
 WHERE true
    ON CONFLICT (subject) DO UPDATE SET version = excluded.version
