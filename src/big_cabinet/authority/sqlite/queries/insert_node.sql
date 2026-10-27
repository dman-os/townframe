-- name: insert_node :exec
-- param: public_key Vec<u8>
-- param: sealed_key Vec<u8>
-- param: key_nonce Vec<u8>
-- param: operator_uuid String
-- param: actors_issuer String
INSERT INTO authority_node
     ( node_id
     , public_key
     , sealed_key
     , key_nonce
     , operator_uuid
     , actors_issuer
     )
VALUES ( 1
       , :public_key
       , :sealed_key
       , :key_nonce
       , :operator_uuid
       , :actors_issuer
       );
