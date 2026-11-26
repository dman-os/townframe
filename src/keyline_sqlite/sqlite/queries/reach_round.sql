-- name: reach_round :exec
-- param: epoch i64
-- param: prev i64
-- One semi-naive round of stratum 1: every fact whose premises are present and one of them is
-- new (stamped `prev`). Three cases: feed 1 new; feed 2 new with feed 1 the subject itself;
-- feed 2 new with feed 1 stored. Each is one join over the delta, not over the relation.
INSERT INTO keyline_reach (s, n, lvl, epoch)
SELECT s
     , n
     , lvl
     , :epoch
  FROM (
         SELECT f1.s AS s
              , d.audience AS n
              , f1.lvl AS lvl
           FROM keyline_reach f1
           JOIN keyline_certificates d ON d.kind = 0 AND d.subject = f1.n AND f1.lvl <= d.power
          WHERE f1.epoch = :prev
            AND /*COMPOSE*/ 1 = 1
            AND d.audience <> f1.s
            AND /*GROUND*/ (d.issuer = d.subject
                            OR EXISTS (SELECT 1
                                         FROM keyline_reach f2
                                        WHERE f2.s = d.subject
                                          AND f2.n = d.issuer
                                          AND f2.lvl = f1.lvl))
          UNION ALL
         SELECT d.subject
              , d.audience
              , f2.lvl
           FROM keyline_reach f2
           JOIN keyline_certificates d ON d.kind = 0 AND d.subject = f2.s AND d.issuer = f2.n AND f2.lvl <= d.power
          WHERE f2.epoch = :prev
            AND d.audience <> d.subject
          UNION ALL
         SELECT f1.s
              , d.audience
              , f2.lvl
           FROM keyline_reach f2
           JOIN keyline_certificates d ON d.kind = 0 AND d.subject = f2.s AND d.issuer = f2.n AND f2.lvl <= d.power
           JOIN keyline_reach f1 ON f1.n = d.subject AND f1.lvl = f2.lvl
          WHERE f2.epoch = :prev
            AND /*COMPOSE*/ 1 = 1
            AND d.audience <> f1.s
       )
 WHERE true
    ON CONFLICT DO NOTHING
