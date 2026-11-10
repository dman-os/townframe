CREATE TABLE big_blobs_upload_receipts (
                      owner TEXT PRIMARY KEY NOT NULL
                    , digest TEXT NOT NULL
                    , released INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0, 1))
                  ) STRICT;