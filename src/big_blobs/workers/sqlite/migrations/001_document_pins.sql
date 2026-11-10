CREATE TABLE IF NOT EXISTS big_blobs_document_pins (
       doc_id TEXT NOT NULL
     , digest TEXT NOT NULL
     , PRIMARY KEY (doc_id, digest)
) STRICT;
