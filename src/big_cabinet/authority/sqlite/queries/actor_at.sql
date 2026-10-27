-- name: actor_at :optional
-- param: path String
-- returns: uuid String, path String, parent_uuid Option<String>, public_key Vec<u8>, sealed_key Vec<u8>, key_nonce Vec<u8>, certificate Vec<u8>
-- into: crate::authority::store::queries::ActorRow
SELECT uuid
     , path
     , parent_uuid
     , public_key
     , sealed_key
     , key_nonce
     , certificate
  FROM authority_actors
 WHERE path = :path;
