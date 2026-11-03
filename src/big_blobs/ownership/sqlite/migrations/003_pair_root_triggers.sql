CREATE TRIGGER IF NOT EXISTS big_blobs_virtual_pair_dependency_release
AFTER DELETE ON big_blobs_byte_dependencies
BEGIN
    DELETE FROM big_blobs_virtual_pairs WHERE digest = OLD.digest;
END;

CREATE TRIGGER IF NOT EXISTS big_blobs_virtual_pair_preclaim_release
AFTER DELETE ON blob_pair_root
WHEN NOT EXISTS (
    SELECT 1
      FROM big_blobs_virtual_pairs AS pair
      JOIN big_blobs_byte_dependencies AS dependency ON dependency.digest = pair.digest
     WHERE pair.cipher_hash = OLD.cipher_hash
)
BEGIN
    DELETE FROM big_blobs_virtual_pairs WHERE cipher_hash = OLD.cipher_hash;
END;
