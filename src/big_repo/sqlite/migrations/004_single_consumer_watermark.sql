-- One durable writer per admission-log consumer.
--
-- `cursors` held a second, per-consumer copy of "reconciled through N" that was
-- written by a transaction separate from the consumer's own state commit: the
-- delta walker acked into `delta_walker_progress` and the cursor row only followed
-- when an in-memory outbox was drained. Shutdown is a normal path (the workers are
-- abortable), so an abort between the two left the walker's durable revision ahead
-- of the cursor row forever, and the walker resumes at its own revision without
-- ever re-reading the gap. The walker progress row is now the only writer of that
-- fact, so the duplicate goes.
DROP TABLE cursors;

-- The admission-reader table is now registration only: it names which consumers
-- gate admission-log retention, and the value comes from `delta_walker_progress`,
-- joined by the `"{namespace}/{consumer_id}"` reader id. SQLite refuses to drop a
-- column an index still names, so the index over it is dropped and recreated on
-- the registration key alone.
DROP INDEX big_repo_keyhive_admission_readers_scope_idx;

ALTER TABLE big_repo_keyhive_admission_readers DROP COLUMN seq;

CREATE INDEX big_repo_keyhive_admission_readers_scope_idx
    ON big_repo_keyhive_admission_readers(scope_id);

-- Dead schema: nothing reads or writes it (no query in the tree and no `.sqlx`
-- entry mentions it), so it only costs a table on every store.
DROP TABLE big_repo_sync_commits_watermark;

-- The store's own queries read this table: `admission_consumer_progress` resolves a
-- consumer's reconciled-through value from it, and `prune_admitted_events` takes its
-- retention floor from it. A store must therefore be able to answer them without
-- assuming a walker has already been constructed in this database -- the walker's own
-- `CREATE TABLE IF NOT EXISTS` is kept for the databases that do not carry these
-- migrations, and the definition below must stay byte-compatible with it.
CREATE TABLE IF NOT EXISTS delta_walker_progress (
      namespace TEXT NOT NULL
    , consumer_id TEXT NOT NULL
    , upstream_revision INTEGER NOT NULL
    , PRIMARY KEY(namespace, consumer_id)
) STRICT;
