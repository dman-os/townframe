-- name: insert_edge :exec
-- param: parent_id String
-- param: child_id String
INSERT INTO big_blobs_discovery_edges (parent_id, child_id)
SELECT :parent_id, :child_id
 WHERE NOT EXISTS (
       SELECT 1 FROM big_blobs_discovery_edges
        WHERE parent_id = :parent_id AND child_id = :child_id
 );
