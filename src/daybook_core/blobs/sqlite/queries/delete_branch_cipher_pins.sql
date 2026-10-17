-- name: delete_branch_cipher_pins :exec
-- param: doc_id String
-- param: branch_id String
DELETE FROM blob_pin_cipher_doc_state WHERE doc_id = :doc_id AND branch_id = :branch_id
