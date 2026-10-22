-- name: insert_item :exec
-- param: name String
-- param: payload Option<Vec<u8>>
-- param: score Option<f64>
INSERT INTO items (name, payload, score) VALUES (:name, :payload, :score);
