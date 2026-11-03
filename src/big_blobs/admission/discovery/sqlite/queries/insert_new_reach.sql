-- name: insert_new_reach :exec
INSERT INTO big_blobs_discovery_reach
SELECT * FROM big_blobs_discovery_next_reach
EXCEPT SELECT * FROM big_blobs_discovery_reach;
