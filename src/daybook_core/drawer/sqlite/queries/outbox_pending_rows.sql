-- name: outbox_pending_rows :many
-- into: crate::drawer::outbox::OutboxDbRow
-- returns: idempotency_key String, branch_doc_id Vec<u8>, entry Vec<u8>, staged_branch_heads Vec<u8>, state String, temporary bool
SELECT idempotency_key
     , branch_doc_id
     , entry
     , staged_branch_heads
     , state
     , temporary
  FROM drawer_add_outbox
 WHERE state != 'done'
 ORDER BY created_at
