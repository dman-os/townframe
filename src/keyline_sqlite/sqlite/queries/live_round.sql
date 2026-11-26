-- name: live_round :exec
-- param: epoch i64
-- param: prev i64
-- One semi-naive round of stratum 2. A hop `d` conducts `s -> d.audience` in context X when
-- feed 1 (`s` stands over `d.subject` in X), feed 2 (`d.issuer` stands over `d.subject` in X) and
-- the hop's own route (`d.issuer` over `d.subject` in the context O of `d` itself, rooted at
-- `d.subject`: the daisy-chain rule) all hold, `d` is not killed outright, and X avoids the
-- audience. A node stands over itself unless the context excludes it. Five branches, one per
-- premise that may be new: feed 1; feed 2 with feed 1 the root itself or stored; the own route
-- with the same two cases for feed 1. The /*OWN*/, /*EXCL*/ and /*COMPOSE*/ markers are where the
-- suite plants bugs (`suite/demand.rs`).
INSERT INTO keyline_live (s, ctx, n, lvl, epoch)
SELECT s
     , ctx
     , n
     , lvl
     , :epoch
  FROM (
         SELECT f1.s AS s
              , f1.ctx AS ctx
              , d.audience AS n
              , f1.lvl AS lvl
           FROM keyline_live f1
           JOIN keyline_certificates d ON d.subject = f1.n AND f1.lvl <= d.power
           LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
          WHERE f1.epoch = :prev
            AND /*COMPOSE*/ 1 = 1
            AND d.audience <> f1.s
            AND COALESCE(dp.total, 0) = 0
            AND ((d.issuer = d.subject AND NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = f1.ctx AND e.node = d.subject))
                 OR EXISTS (SELECT 1 FROM keyline_live f2
                             WHERE f2.s = d.subject AND f2.ctx = f1.ctx AND f2.n = d.issuer AND f2.lvl = f1.lvl))
            AND /*OWN*/ ((d.issuer = d.subject AND NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = COALESCE(dp.ctx, 0) AND e.node = d.subject))
                 OR EXISTS (SELECT 1 FROM keyline_live fo
                             WHERE fo.s = d.subject AND fo.ctx = COALESCE(dp.ctx, 0) AND fo.n = d.issuer AND fo.lvl = f1.lvl))
            AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = f1.ctx AND e.node = d.audience))
          UNION ALL
         SELECT d.subject
              , f2.ctx
              , d.audience
              , f2.lvl
           FROM keyline_live f2
           JOIN keyline_certificates d ON d.subject = f2.s AND d.issuer = f2.n AND f2.lvl <= d.power
           LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
          WHERE f2.epoch = :prev
            AND d.audience <> d.subject
            AND COALESCE(dp.total, 0) = 0
            AND NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = f2.ctx AND e.node = d.subject)
            AND /*OWN*/ (EXISTS (SELECT 1 FROM keyline_live fo
                                  WHERE fo.s = d.subject AND fo.ctx = COALESCE(dp.ctx, 0) AND fo.n = d.issuer AND fo.lvl = f2.lvl))
            AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = f2.ctx AND e.node = d.audience))
          UNION ALL
         SELECT f1.s
              , f2.ctx
              , d.audience
              , f2.lvl
           FROM keyline_live f2
           JOIN keyline_certificates d ON d.subject = f2.s AND d.issuer = f2.n AND f2.lvl <= d.power
           JOIN keyline_live f1 ON f1.ctx = f2.ctx AND f1.n = d.subject AND f1.lvl = f2.lvl
           JOIN keyline_root rt ON rt.s = f1.s
           LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
          WHERE f2.epoch = :prev
            AND /*COMPOSE*/ 1 = 1
            AND d.audience <> f1.s
            AND COALESCE(dp.total, 0) = 0
            AND /*OWN*/ (EXISTS (SELECT 1 FROM keyline_live fo
                                  WHERE fo.s = d.subject AND fo.ctx = COALESCE(dp.ctx, 0) AND fo.n = d.issuer AND fo.lvl = f2.lvl))
            AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = f2.ctx AND e.node = d.audience))
          UNION ALL
         SELECT d.subject
              , x.ctx
              , d.audience
              , fo.lvl
           FROM keyline_live fo
           JOIN keyline_certificates d ON d.subject = fo.s AND d.issuer = fo.n AND fo.lvl <= d.power
           LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
          CROSS JOIN keyline_ctx_use x
          WHERE fo.epoch = :prev
            AND fo.ctx = COALESCE(dp.ctx, 0)
            AND d.audience <> d.subject
            AND COALESCE(dp.total, 0) = 0
            AND NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = x.ctx AND e.node = d.subject)
            AND EXISTS (SELECT 1 FROM keyline_live f2
                         WHERE f2.s = d.subject AND f2.ctx = x.ctx AND f2.n = d.issuer AND f2.lvl = fo.lvl)
            AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = x.ctx AND e.node = d.audience))
          UNION ALL
         SELECT f1.s
              , x.ctx
              , d.audience
              , fo.lvl
           FROM keyline_live fo
           JOIN keyline_certificates d ON d.subject = fo.s AND d.issuer = fo.n AND fo.lvl <= d.power
           LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
          CROSS JOIN keyline_ctx_use x
           JOIN keyline_live f1 ON f1.ctx = x.ctx AND f1.n = d.subject AND f1.lvl = fo.lvl
           JOIN keyline_root rt ON rt.s = f1.s
          WHERE fo.epoch = :prev
            AND /*COMPOSE*/ 1 = 1
            AND fo.ctx = COALESCE(dp.ctx, 0)
            AND d.audience <> f1.s
            AND COALESCE(dp.total, 0) = 0
            AND EXISTS (SELECT 1 FROM keyline_live f2
                         WHERE f2.s = d.subject AND f2.ctx = x.ctx AND f2.n = d.issuer AND f2.lvl = fo.lvl)
            AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = x.ctx AND e.node = d.audience))
       )
 WHERE true
    ON CONFLICT DO NOTHING
