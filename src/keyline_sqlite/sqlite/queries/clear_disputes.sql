-- name: clear_disputes :exec
DELETE FROM keyline_dispute
 WHERE delegation IN (SELECT d.digest
                        FROM keyline_certificates d
                       WHERE d.subject IN (SELECT s FROM keyline_root))
