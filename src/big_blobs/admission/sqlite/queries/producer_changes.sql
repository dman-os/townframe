-- name: producer_changes :many
-- param: inventory_id String
-- returns: digest String
WITH desired AS (
    SELECT digest
         , json_array(doc_id, drawer_id, root, name) AS owner
      FROM big_blobs_writer_claims
     WHERE inventory_id = :inventory_id
)
SELECT digest
  FROM (
        SELECT digest, owner FROM desired
        EXCEPT
        SELECT digest, owner
          FROM big_blobs_producer_claims
         WHERE inventory_id = :inventory_id
       )
 UNION
SELECT digest
  FROM (
        SELECT digest, owner
          FROM big_blobs_producer_claims
         WHERE inventory_id = :inventory_id
        EXCEPT
        SELECT digest, owner FROM desired
       )
