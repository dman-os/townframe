-- name: init_done :optional
-- param: init_id String
-- returns: init_id String
SELECT init_id FROM init_per_node WHERE init_id = :init_id
