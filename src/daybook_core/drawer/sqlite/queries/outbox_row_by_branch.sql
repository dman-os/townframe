-- name: outbox_row_by_branch :optional
-- param: branch_doc_id Vec<u8>
-- into: crate::drawer::outbox::OutboxDbRow
-- returns: idempotency_key String, branch_doc_id Vec<u8>, entry Vec<u8>, staged_branch_heads Vec<u8>, state String, temporary bool
SELECT idempotency_key
     , branch_doc_id
     , entry
     , staged_branch_heads
     , state
     , temporary
  FROM drawer_add_outbox
 WHERE branch_doc_id = :branch_doc_id LIMIT 1
