-- name: mark_effects :exec
INSERT OR IGNORE INTO big_blobs_byte_effects (digest)
SELECT digest FROM big_blobs_byte_owners
 UNION SELECT digest FROM big_blobs_byte_dependencies
 UNION SELECT plaintext FROM big_blobs_byte_dependencies
 UNION SELECT digest FROM big_blobs_byte_pin_roots;
