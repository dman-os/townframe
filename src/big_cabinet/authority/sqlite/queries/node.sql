-- name: node :optional
-- returns: public_key Vec<u8>, sealed_key Vec<u8>, key_nonce Vec<u8>, operator_uuid String, actors_issuer String
SELECT public_key
     , sealed_key
     , key_nonce
     , operator_uuid
     , actors_issuer
  FROM authority_node
 WHERE node_id = 1;
