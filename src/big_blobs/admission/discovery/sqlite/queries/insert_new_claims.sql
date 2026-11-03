-- name: insert_new_claims :exec
INSERT INTO big_blobs_discovery_claims
SELECT * FROM big_blobs_discovery_next_claims
EXCEPT SELECT * FROM big_blobs_discovery_claims;
