-- The route table is the facet-set side of the FacetDelta current state:
-- membership is keyed by the complete document/branch/tag/key identity, while
-- heads and provenance remain typed JSON for exact head consumers.
CREATE TABLE IF NOT EXISTS facet_set_doc_facets (
    document_id TEXT NOT NULL
  , branch_id TEXT NOT NULL
  , facet_tag TEXT NOT NULL
  , facet_id TEXT NOT NULL
  , facet_heads_json TEXT NOT NULL
  , actor_id_json TEXT NOT NULL
  , branch_heads_json TEXT NOT NULL
  , PRIMARY KEY(document_id, branch_id, facet_tag, facet_id)
) STRICT;
CREATE INDEX IF NOT EXISTS facet_set_doc_facets_tag_id
    ON facet_set_doc_facets (facet_tag, facet_id);
CREATE INDEX IF NOT EXISTS idx_facet_set_doc_facets_route
    ON facet_set_doc_facets(facet_tag, facet_id, document_id, branch_id);

-- The presence-plane projection (ADR 003 §13): rows carry the plaintext
-- digest the `Blob` facet VALUE names, not the facet key's id, which is
-- `DEFAULT_FACET_ID` for every production-authored `Blob` facet. Projected
-- alongside the route table in the streaming projection write.
CREATE TABLE IF NOT EXISTS facet_set_doc_blob_facets (
    document_id TEXT NOT NULL
  , branch_id TEXT NOT NULL
  , blob_digest TEXT NOT NULL
  , PRIMARY KEY(document_id, branch_id, blob_digest)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_facet_set_doc_blob_facets_digest
    ON facet_set_doc_blob_facets(blob_digest, document_id, branch_id);
