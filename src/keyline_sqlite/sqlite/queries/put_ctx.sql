-- name: put_ctx :exec
-- param: signature Vec<u8>
INSERT INTO keyline_ctx (signature)
VALUES (:signature)
    ON CONFLICT DO NOTHING
