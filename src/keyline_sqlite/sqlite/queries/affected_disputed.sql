-- name: affected_disputed :exec
-- The disputed subjects whose live set the batch can change: a touched subject, or one that stands
-- over a touched subject (its closure holds it).
INSERT INTO keyline_affected (subject)
SELECT subject
  FROM (
         SELECT t.subject AS subject
           FROM keyline_touched t
          WHERE EXISTS (SELECT 1 FROM keyline_disputed dp WHERE dp.subject = t.subject)
          UNION
         SELECT r.s
           FROM keyline_reach r
          WHERE r.n IN (SELECT subject FROM keyline_touched)
            AND r.lvl = 0
            AND EXISTS (SELECT 1 FROM keyline_disputed dp WHERE dp.subject = r.s)
       )
 WHERE true
    ON CONFLICT DO NOTHING
