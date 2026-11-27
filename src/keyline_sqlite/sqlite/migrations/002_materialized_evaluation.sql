-- The evaluation, materialized. Stratum 1 (positive reach) is stored and extended at insert;
-- stratum 2 (the live set) is computed per disputed subject and stored with the version it was
-- computed at. See `docs/scratch/hive-store-design.md`.

-- The four standing levels as ranks (0 Relay, 1 Read, 2 Edit, 3 Admin): the threshold encoding
-- joins every rule against this table once per level.
CREATE TABLE keyline_level (
      lvl INTEGER PRIMARY KEY CHECK (lvl BETWEEN 0 AND 3)
) STRICT, WITHOUT ROWID;

INSERT INTO keyline_level (lvl) VALUES (0), (1), (2), (3);

-- Fixpoint rounds are numbered by a counter that never repeats: a round's delta is the rows
-- stamped with the previous round's number.
ALTER TABLE keyline_meta ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;

-- Certificates of the batch being inserted, with the columns the rounds read: the delta of round
-- zero, scanned as the batch it is. Empty outside a transaction.
CREATE TABLE keyline_new (
      kind INTEGER NOT NULL
    , digest BLOB NOT NULL
    , issuer BLOB NOT NULL
    , audience BLOB
    , subject BLOB
    , power INTEGER
    , revoke BLOB
    , PRIMARY KEY (kind, digest)
) STRICT, WITHOUT ROWID;

-- Stratum 1. `(s, n, lvl)` means node `n` has standing of at least `lvl` over subject `s` with
-- every revocation ignored: the threshold form of `reaches`. A node's standing over itself
-- (Admin) is implicit and never stored, so `n <> s`. Append-only: nothing is ever deleted, which
-- is what makes extending it at insert an incremental computation. `epoch` is the round that
-- derived the row.
CREATE TABLE keyline_reach (
      s BLOB NOT NULL CHECK (length(s) = 32)
    , n BLOB NOT NULL CHECK (length(n) = 32)
    , lvl INTEGER NOT NULL CHECK (lvl BETWEEN 0 AND 3)
    , epoch INTEGER NOT NULL
    , PRIMARY KEY (s, n, lvl)
    , CHECK (s <> n)
) STRICT, WITHOUT ROWID;

-- Who stands over a node (the feed of every composition, and admin reach: `lvl = 3`).
CREATE INDEX keyline_reach_by_target
    ON keyline_reach (n, lvl, s);

-- A round's delta.
CREATE INDEX keyline_reach_by_epoch
    ON keyline_reach (epoch);

-- Indexes over certificates are partial on the column they key, not on `kind`: a query that
-- equates `subject` (or `revoke`) implies it is not null, so the planner can use the partial index,
-- whereas it prefers the primary key's `kind` range whenever `kind = 0` is spelled out (it scans
-- every delegation to keep a few). Delegations are the rows with a subject; revocations the rows
-- with a target.
DROP INDEX keyline_delegations_by_subject_audience;
CREATE INDEX keyline_delegations_by_subject_audience
    ON keyline_certificates (subject, audience)
 WHERE subject IS NOT NULL;

DROP INDEX keyline_revocations_by_target;
CREATE INDEX keyline_revocations_by_target
    ON keyline_certificates (revoke, issuer)
 WHERE revoke IS NOT NULL;

-- The delegations a node issued about one subject: feed 2 of a round.
CREATE INDEX keyline_delegations_by_subject_issuer
    ON keyline_certificates (subject, issuer)
 WHERE subject IS NOT NULL;

-- Subjects with a delegation some revocation names. A revocation that names a delegation not yet
-- in the set marks its subject when the delegation arrives.
CREATE TABLE keyline_dsubject (
      subject BLOB PRIMARY KEY CHECK (length(subject) = 32)
    , epoch INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX keyline_dsubject_by_epoch
    ON keyline_dsubject (epoch);

-- Subjects whose closure holds such a delegation: the only subjects whose live set can differ
-- from their positive reach. Monotone like `keyline_reach`.
CREATE TABLE keyline_disputed (
      subject BLOB PRIMARY KEY CHECK (length(subject) = 32)
) STRICT, WITHOUT ROWID;

-- Exclusion contexts, keyed by the canonical signature of the excluded set (its node ids in
-- order, concatenated), so delegations revoked by the same signers share one. Context 0 is the
-- empty set. Append-only.
CREATE TABLE keyline_ctx (
      ctx INTEGER PRIMARY KEY
    , signature BLOB NOT NULL UNIQUE
) STRICT;

INSERT INTO keyline_ctx (ctx, signature) VALUES (0, x'');

CREATE TABLE keyline_excl (
      ctx INTEGER NOT NULL
    , node BLOB NOT NULL
    , PRIMARY KEY (ctx, node)
) STRICT, WITHOUT ROWID;

-- What revocations did to one delegation, for the delegations a stratum 2 computation reads:
-- the context its own route must avoid, and whether an issuer's or audience's own revocation
-- kills it outright. Rewritten by each computation for the delegations it covers.
CREATE TABLE keyline_dispute (
      delegation BLOB PRIMARY KEY CHECK (length(delegation) = 32)
    , ctx INTEGER NOT NULL
    , total INTEGER NOT NULL CHECK (total IN (0, 1))
) STRICT, WITHOUT ROWID;

-- The roots and contexts of the stratum 2 computation in progress. Empty outside a transaction.
CREATE TABLE keyline_root (
      s BLOB PRIMARY KEY
) STRICT, WITHOUT ROWID;

CREATE TABLE keyline_ctx_use (
      ctx INTEGER PRIMARY KEY
) STRICT, WITHOUT ROWID;

-- Stratum 2, maintained at insert for every disputed subject the insert can change (so a read
-- never computes it). `(ctx, s, n, lvl)` means `n` has live standing of at least `lvl` over `s` on routes
-- that avoid the nodes of context `ctx`. `n <> s`; a root's standing over itself is implicit
-- unless the context excludes it. Rows for a root are replaced whenever it is recomputed.
CREATE TABLE keyline_live (
      s BLOB NOT NULL
    , ctx INTEGER NOT NULL
    , n BLOB NOT NULL
    , lvl INTEGER NOT NULL CHECK (lvl BETWEEN 0 AND 3)
    , epoch INTEGER NOT NULL
    , PRIMARY KEY (s, ctx, n, lvl)
) STRICT, WITHOUT ROWID;

CREATE INDEX keyline_live_by_target
    ON keyline_live (n, lvl, ctx, s);

CREATE INDEX keyline_live_by_epoch
    ON keyline_live (epoch);

-- Scratch of the insert in progress: the subjects the batch touched, and the disputed subjects
-- whose live set it can change. Empty outside a transaction.
CREATE TABLE keyline_touched (
      subject BLOB PRIMARY KEY
) STRICT, WITHOUT ROWID;

CREATE TABLE keyline_affected (
      subject BLOB PRIMARY KEY
) STRICT, WITHOUT ROWID;

-- The revocations a node issued: a node that gains admin reach extends their coverage.
CREATE INDEX keyline_revocations_by_issuer
    ON keyline_certificates (issuer)
 WHERE revoke IS NOT NULL;
