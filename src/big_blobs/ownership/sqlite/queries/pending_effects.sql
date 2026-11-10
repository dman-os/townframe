-- name: pending_effects :many
-- returns: digest String
SELECT digest
  FROM big_blobs_byte_effects
 ORDER BY digest;
