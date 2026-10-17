-- name: outbox_insert_pending :exec
-- param: idempotency_key String
-- param: branch_doc_id Vec<u8>
-- param: entry Vec<u8>
-- param: staged_branch_heads Vec<u8>
-- param: temporary bool
-- param: created_at i64
INSERT INTO drawer_add_outbox (
    idempotency_key
  , branch_doc_id
  , entry
  , staged_branch_heads
  , state
  , temporary
  , created_at
) VALUES (:idempotency_key, :branch_doc_id, :entry, :staged_branch_heads, 'pending-add', :temporary, :created_at)
