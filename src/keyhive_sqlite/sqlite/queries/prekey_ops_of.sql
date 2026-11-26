-- name: prekey_ops_of :many
-- param: individual_id Vec<u8>
-- returns: digest Vec<u8>, signed Vec<u8>
SELECT digest
     , signed
  FROM kh_prekey_op
 WHERE individual_id = :individual_id
