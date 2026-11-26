-- name: clear_disputes :exec
DELETE FROM keyline_dispute
 WHERE delegation IN (SELECT d.digest
                        FROM keyline_root rt
                        JOIN keyline_certificates d ON d.kind = 0 AND d.subject = rt.s)
