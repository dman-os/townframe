-- An upload hold keeps its bytes for a grace period unless a document claims them first
-- (ADR 042 §6). `held_at` is when the hold was taken, in unix seconds; the holds that exist when
-- this migration runs start their grace period now.
ALTER TABLE big_blobs_upload_receipts ADD COLUMN held_at INTEGER NOT NULL DEFAULT 0;

UPDATE big_blobs_upload_receipts
   SET held_at = CAST(strftime('%s', 'now') AS INTEGER);

CREATE INDEX big_blobs_upload_receipts_held_at ON big_blobs_upload_receipts (held_at) WHERE released = 0;
