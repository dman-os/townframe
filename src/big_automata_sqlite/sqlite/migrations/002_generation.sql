-- The store's generation and the cursor table's generation column. SQLite cannot add a NOT NULL
-- column with a computed default, so both tables are rebuilt.

-- The meta row gains the store's generation (see `big_automata::part_store::Generation`): the
-- identity of this log, the 16 bytes of a UUID. It is minted here, by the migration, so a
-- database has a generation from the moment it has a log (a new file runs this right after
-- 001; a file made before generations is given one when it is upgraded) and no open ever has
-- to decide whether to mint. `mint_generation` replaces it. A copy of the file carries it
-- along, which is why a restore needs the operator's mint.
CREATE TABLE big_automata_meta_new (
      id INTEGER PRIMARY KEY CHECK (id = 1)
    , rev INTEGER NOT NULL
    , generation BLOB NOT NULL CHECK (length(generation) = 16)
) STRICT;

INSERT INTO big_automata_meta_new (id, rev, generation)
SELECT id
     , rev
     , randomblob(16)
  FROM big_automata_meta;

DROP TABLE big_automata_meta;

ALTER TABLE big_automata_meta_new RENAME TO big_automata_meta;

-- The driver's `PeerCursors` now stores a `PeerCursor`: the rev and the generation of the
-- peer's log it is a position in, together and both required. The table is last-write-wins: a
-- write replaces the row whatever its rev or generation. The rows written before generations
-- have none, so they are not carried over: a cursor with no generation cannot say which log it
-- is a position in, and a part with no stored cursor restarts from the beginning, which is
-- always correct.
DROP TABLE big_automata_peer_cursors;

CREATE TABLE big_automata_peer_cursors (
      peer BLOB NOT NULL
    , backend TEXT NOT NULL
    , part_key BLOB NOT NULL
    , cursor INTEGER NOT NULL
    , generation BLOB NOT NULL CHECK (length(generation) = 16)
    , PRIMARY KEY (peer, backend, part_key)
) STRICT, WITHOUT ROWID;
