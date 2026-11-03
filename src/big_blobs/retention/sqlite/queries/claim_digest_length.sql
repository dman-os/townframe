-- name: claim_digest_length :many
-- param: inventory_id String
-- param: digest String
-- returns: length_octets i64
SELECT length_octets FROM big_blobs_writer_claims
 WHERE inventory_id = :inventory_id AND digest = :digest;
