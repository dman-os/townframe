-- name: begin_command :exec
-- param: command_id String
-- param: kind String
-- param: request_digest Vec<u8>
-- param: actor_uuid String
-- param: doc_id Option<String>
-- param: placement Option<String>
-- param: detail_json Option<String>
-- param: created_at i64
INSERT INTO big_cabinet_commands
     ( command_id
     , kind
     , request_digest
     , actor_uuid
     , doc_id
     , placement
     , detail_json
     , state
     , heads_json
     , created_at
     )
VALUES ( :command_id
       , :kind
       , :request_digest
       , :actor_uuid
       , :doc_id
       , :placement
       , :detail_json
       , 'pending'
       , '[]'
       , :created_at
       )
ON CONFLICT (command_id) DO NOTHING;
