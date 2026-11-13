CREATE INDEX big_blobs_upload_receipts_held_digest ON big_blobs_upload_receipts (digest) WHERE released = 0;
