-- The hive's sedimentree storage. One hive per database: no table is keyed by a scope.
--
-- Storage is the minimized tree. A write minimizes the tree and deletes what the minimization
-- covers in the same transaction, so the rows here are exactly the loose commits and fragments
-- of the tree's minimal form (nothing a fragment covers, no fragment a deeper one dominates),
-- and at most one payload per head: when two payloads share a head, the lowest digest is kept,
-- as `Sedimentree` itself decides.

-- One row per tree id ever written. `rev` is drawn from one sequence for the whole hive (the
-- next value is the table's maximum plus one, which is safe because there is one writer) and
-- moves in every transaction that changes the tree, inside that transaction. A rollback leaves
-- it where it was, and every snapshot reads it together with the rows it describes. It never
-- repeats and never goes down, which is what makes it the tree's generation (subduction's
-- `MinimizedTreeStore` rule 3) and what lets a reader tail "which trees changed since rev N".
--
-- `live` is 0 for a deleted tree. The row stays as a tombstone: deleting it would let a later
-- registration start the tree's revs over, and a reader that held the old rev would match it.
-- The rows accumulate, one per tree id ever written, like the keys of the tables below.
CREATE TABLE sedimentree_trees (
      tree_id BLOB NOT NULL PRIMARY KEY CHECK (length(tree_id) = 32)
    , rev INTEGER NOT NULL CHECK (rev > 0)
    , live INTEGER NOT NULL CHECK (live IN (0, 1))
) STRICT, WITHOUT ROWID;

-- The change feed and the next rev both read this index: `rev > N ORDER BY rev` and `MAX(rev)`.
CREATE UNIQUE INDEX sedimentree_trees_by_rev
    ON sedimentree_trees (rev);

-- Column order is a contract. A row is stored in the order of its columns, and SQLite (like
-- Postgres TOAST) only follows a large value off the page when a query reads it. The metadata
-- the minimization needs (`signed`, the encoded loose commit) sits before `blob`, so a
-- metadata read never touches the blob's overflow pages. Never put a column after `blob`, and
-- do not move `blob` earlier. Not `WITHOUT ROWID`: that format keeps whole rows in the
-- primary key's b-tree, where large inline values hurt.
--
-- `commit_id` is the commit's causal head, the identity subduction addresses it by; `digest`
-- is the hash of its payload. There is no foreign key to `sedimentree_trees`: the writer
-- registers the tree in the same transaction as its first row, and `suite/invariants.rs`
-- checks that no row lacks a live tree.
--
-- `pcs_update_hash` is the hash of the PCS update the commit's ciphertext was encrypted under,
-- passed in by whoever encrypted it, NULL for a plaintext commit. Nothing decodes the blob to
-- learn it.
CREATE TABLE sedimentree_commits (
      tree_id BLOB NOT NULL CHECK (length(tree_id) = 32)
    , commit_id BLOB NOT NULL CHECK (length(commit_id) = 32)
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
    , pcs_update_hash BLOB CHECK (length(pcs_update_hash) = 32)
    , signed BLOB NOT NULL
    , blob BLOB NOT NULL
    , PRIMARY KEY (tree_id, commit_id)
) STRICT;

-- The fragments, the same shape and the same column-order contract; `fragment_head` is the
-- fragment's head commit.
CREATE TABLE sedimentree_fragments (
      tree_id BLOB NOT NULL CHECK (length(tree_id) = 32)
    , fragment_head BLOB NOT NULL CHECK (length(fragment_head) = 32)
    , digest BLOB NOT NULL CHECK (length(digest) = 32)
    , pcs_update_hash BLOB CHECK (length(pcs_update_hash) = 32)
    , signed BLOB NOT NULL
    , blob BLOB NOT NULL
    , PRIMARY KEY (tree_id, fragment_head)
) STRICT;

-- Ciphertexts under one PCS update: only encrypted rows are in it.
CREATE INDEX sedimentree_commits_by_pcs_update
    ON sedimentree_commits (tree_id, pcs_update_hash)
 WHERE pcs_update_hash IS NOT NULL;

CREATE INDEX sedimentree_fragments_by_pcs_update
    ON sedimentree_fragments (tree_id, pcs_update_hash)
 WHERE pcs_update_hash IS NOT NULL;
