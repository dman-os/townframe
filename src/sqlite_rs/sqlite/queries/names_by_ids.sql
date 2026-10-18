-- name: names_by_ids :many
-- param: ids Json<Vec<i64>>
-- returns: name String
SELECT name
  FROM items
 WHERE id IN (SELECT value FROM json_each(:ids))
 ORDER BY id;
