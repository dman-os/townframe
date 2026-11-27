-- name: disputed_from_dsubjects :exec
-- param: epoch i64
-- A newly marked subject is disputed, and so is everything that stands over it in the positive
-- graph (its closure holds the delegation).
INSERT INTO keyline_disputed (subject)
SELECT subject
  FROM (
         SELECT ds.subject AS subject
           FROM keyline_dsubject ds
          WHERE ds.epoch = :epoch
          UNION
         SELECT r.s
           FROM keyline_dsubject ds
           JOIN keyline_reach r ON r.n = ds.subject AND r.lvl = 0
          WHERE ds.epoch = :epoch
       )
 WHERE true
    ON CONFLICT DO NOTHING
