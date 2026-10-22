-- name: get_item :optional
-- param: id i64
-- returns: id i64, name String, payload Option<Vec<u8>>, score Option<f64>
SELECT id, name, payload, score FROM items WHERE id = :id;
