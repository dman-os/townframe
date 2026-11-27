-- name: ctx_of :one
-- param: signature Vec<u8>
-- returns: ctx i64
SELECT ctx
  FROM keyline_ctx
 WHERE signature = :signature
