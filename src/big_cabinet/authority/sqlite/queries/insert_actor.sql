-- name: insert_actor :exec
-- param: uuid String
-- param: path String
-- param: parent_uuid Option<String>
-- param: public_key Vec<u8>
-- param: sealed_key Vec<u8>
-- param: key_nonce Vec<u8>
-- param: certificate Vec<u8>
INSERT INTO authority_actors
     ( uuid
     , path
     , parent_uuid
     , public_key
     , sealed_key
     , key_nonce
     , certificate
     )
VALUES ( :uuid
       , :path
       , :parent_uuid
       , :public_key
       , :sealed_key
       , :key_nonce
       , :certificate
       );
