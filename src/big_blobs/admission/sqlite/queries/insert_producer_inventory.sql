-- name: insert_producer_inventory :exec
-- param: inventory_id String
INSERT INTO big_blobs_producer_claims (inventory_id, digest, owner)
SELECT inventory_id
     , digest
     , json_array(doc_id, drawer_id, root, name)
  FROM big_blobs_writer_claims
 WHERE inventory_id = :inventory_id
