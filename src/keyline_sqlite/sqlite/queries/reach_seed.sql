-- name: reach_seed :exec
-- param: epoch i64
-- Round zero of stratum 1: the delegations of the batch, against the reach already stored.
-- `reaches(L, S, Aud) :- delegation(_, Iss, Aud, Sub, Pow), le(L, Pow), reaches(L, S, Sub),
-- reaches(L, Sub, Iss)`, where a node reaches itself and so `S = Sub` or `Sub = Iss` needs no row.
-- The /*GROUND*/ and /*COMPOSE*/ markers are where the suite plants bugs (`suite/demand.rs`).
INSERT INTO keyline_reach (s, n, lvl, epoch)
SELECT s
     , n
     , lvl
     , :epoch
  FROM (
         SELECT d.subject AS s
              , d.audience AS n
              , lv.lvl AS lvl
           FROM keyline_new d
           JOIN keyline_level lv ON lv.lvl <= d.power
          WHERE d.kind = 0
            AND d.audience <> d.subject
            AND /*GROUND*/ (d.issuer = d.subject
                            OR EXISTS (SELECT 1
                                         FROM keyline_reach f2
                                        WHERE f2.s = d.subject
                                          AND f2.n = d.issuer
                                          AND f2.lvl = lv.lvl))
          UNION ALL
         SELECT f1.s
              , d.audience
              , lv.lvl
           FROM keyline_new d
           JOIN keyline_level lv ON lv.lvl <= d.power
           JOIN keyline_reach f1 ON f1.n = d.subject AND f1.lvl = lv.lvl
          WHERE d.kind = 0
            AND /*COMPOSE*/ 1 = 1
            AND d.audience <> f1.s
            AND /*GROUND*/ (d.issuer = d.subject
                            OR EXISTS (SELECT 1
                                         FROM keyline_reach f2
                                        WHERE f2.s = d.subject
                                          AND f2.n = d.issuer
                                          AND f2.lvl = lv.lvl))
       )
 WHERE true
    ON CONFLICT DO NOTHING
