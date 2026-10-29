-- name: get_command :optional
-- param: command_id String
-- returns: command_id String, kind String, request_digest Vec<u8>, actor_uuid String, doc_id Option<String>, placement Option<String>, detail_json Option<String>, state String, heads_json String
SELECT command_id
     , kind
     , request_digest
     , actor_uuid
     , doc_id
     , placement
     , detail_json
     , state
     , heads_json
  FROM big_cabinet_commands
 WHERE command_id = :command_id;
