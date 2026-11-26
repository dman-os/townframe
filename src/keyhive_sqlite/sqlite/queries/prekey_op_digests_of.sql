-- name: prekey_op_digests_of :many
-- param: individual_id Vec<u8>
-- returns: digest Vec<u8>
SELECT digest
  FROM kh_prekey_op
 WHERE individual_id = :individual_id
