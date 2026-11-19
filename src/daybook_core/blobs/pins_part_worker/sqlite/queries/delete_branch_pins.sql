-- name: delete_branch_pins :exec
-- param: doc_id String
-- param: branch_id String
DELETE FROM doc_blob_pins WHERE doc_id = :doc_id AND branch_id = :branch_id
