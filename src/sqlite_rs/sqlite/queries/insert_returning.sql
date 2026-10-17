-- name: insert_returning :one
-- param: name String
-- returns: id i64, name String
INSERT INTO items (name)
VALUES (:name)
RETURNING id
        , name;
