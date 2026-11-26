-- name: live_seed :exec
-- param: epoch i64
-- Round zero of stratum 2: the root edges (a delegation issued by the key it is about) in every
-- context. `live(X, L, S, S)` holds unless X excludes S; a hop must also be live in its own
-- context O, and avoid `Iss` there. A delegation its issuer's or audience's own revocation kills
-- (`total`) conducts nothing.
INSERT INTO keyline_live (s, ctx, n, lvl, epoch)
SELECT d.subject
     , x.ctx
     , d.audience
     , lv.lvl
     , :epoch
  FROM keyline_root rt
  JOIN keyline_certificates d ON d.subject = rt.s AND d.issuer = d.subject
  JOIN keyline_level lv ON lv.lvl <= d.power
 CROSS JOIN keyline_ctx_use x
  LEFT JOIN keyline_dispute dp ON dp.delegation = d.digest
 WHERE COALESCE(dp.total, 0) = 0
   AND d.audience <> d.subject
   AND NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = x.ctx AND e.node = d.subject)
   AND /*OWN*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = COALESCE(dp.ctx, 0) AND e.node = d.subject))
   AND /*EXCL*/ (NOT EXISTS (SELECT 1 FROM keyline_excl e WHERE e.ctx = x.ctx AND e.node = d.audience))
    ON CONFLICT DO NOTHING
