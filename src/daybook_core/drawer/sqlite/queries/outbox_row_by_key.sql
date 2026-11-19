-- name: outbox_row_by_key :optional
-- param: idempotency_key String
-- into: crate::drawer::outbox::OutboxDbRow
-- returns: idempotency_key String, branch_doc_id Vec<u8>, entry Vec<u8>, staged_branch_heads Vec<u8>, state String, temporary bool
SELECT idempotency_key
     , branch_doc_id
     , entry
     , staged_branch_heads
     , state
     , temporary
  FROM drawer_add_outbox
 WHERE idempotency_key = :idempotency_key
