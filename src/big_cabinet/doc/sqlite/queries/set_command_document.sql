-- name: set_command_document :exec
-- param: doc_id String
-- param: command_id String
UPDATE big_cabinet_commands
   SET doc_id = :doc_id
 WHERE command_id = :command_id
   AND state = 'pending'
   AND doc_id IS NULL;
