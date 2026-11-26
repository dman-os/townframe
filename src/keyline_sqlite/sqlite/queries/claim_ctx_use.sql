-- name: claim_ctx_use :exec
-- The contexts of the computation: the empty one, and the one each disputed hop must avoid.
INSERT INTO keyline_ctx_use (ctx)
SELECT ctx
  FROM (
         SELECT 0 AS ctx
          UNION
         SELECT dp.ctx
           FROM keyline_root rt
           JOIN keyline_certificates d ON d.kind = 0 AND d.subject = rt.s
           JOIN keyline_dispute dp ON dp.delegation = d.digest
          WHERE dp.total = 0
       )
 WHERE true
    ON CONFLICT DO NOTHING
