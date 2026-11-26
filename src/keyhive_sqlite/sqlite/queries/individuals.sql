-- name: individuals :many
-- returns: individual_id Vec<u8>
-- A loose index scan: each step seeks the next distinct individual in the primary key, so the
-- cost follows the number of individuals, not of operations.
WITH RECURSIVE ids(individual_id) AS (
    SELECT min(individual_id) FROM kh_prekey_op
    UNION ALL
    SELECT (SELECT min(individual_id) FROM kh_prekey_op WHERE individual_id > ids.individual_id)
      FROM ids
     WHERE ids.individual_id IS NOT NULL
)
SELECT individual_id
  FROM ids
 WHERE individual_id IS NOT NULL
