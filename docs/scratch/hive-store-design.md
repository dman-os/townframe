# Hive store: Keyline from SQL on demand, and the rest of keyhive's state beside it

Status: design for `keyline_sqlite` (implemented on this branch) and for the non-Keyline tables (not
implemented; sketched so the Keyline tables and a follow-up crate share one database and one
transaction). Sources: keyhive fork `origin/townframe-changes` (`design/keyline/implementation.md`,
`evaluation-notes.md` section 7, `keyline_memory::state`), `origin/keyline/hive-store` at `4053d96`
(`keyhive_core/src/store.rs`, the `KeyhiveStore` trait), and the kh-archive report.

## 1. The problem

Today `keyline_sqlite` stores certificates in SQL but answers every evaluated query from a
`keyline_memory::State` over the **whole** set (`cache.rs`): a cache miss reads every row and verifies
every signature; the cache is one set per process. A relay holding many documents pays for all of them to
answer one point query, memory grows with the set, and a second process rebuilds after every foreign
commit. The goal: answer from SQL on demand, hold no whole-set state.

## 2. Evaluation strategies considered

The evaluation program (implementation.md "Evaluation") has two strata. Stratum 1 (`reaches`, admin
reach, coverage) is monotone and its membership rule is non-linear recursion, so it is not a single
`WITH RECURSIVE`. Stratum 2 (live set, caps) negates over stratum 1 and needs a fixed point per exclusion
context.

| | A. Materialize at insert | B. Demand-driven closure + reference evaluator (chosen) | C. SQL-only demand-driven |
|---|---|---|---|
| Insert cost | semi-naive delta of `reaches` (up to k^2 facts for a role ladder), coverage, then retraction-capable maintenance of stratum 2 (DRed or Z-sets) inside the writer's transaction | O(batch log n): one multi-row insert and a version bump | same as B |
| Query cost | one indexed read per question, for the materialized answers | O(closure(S)) rows read, plus the reference evaluation over those rows | O(route) per round in SQL, but stratum 2's contexts need a statement loop in SQL, per context |
| State | tables as large as the derived facts (Omega(output)) | none beyond the certificate table | none |
| Agreement with the reference | by a second implementation of both strata, held to the differential | by construction: the reference evaluator runs, over a set that provably contains everything the answer depends on | by a second implementation of both strata |
| Write contention | long write tx (round loop) holding the single writer | short | short |

**Chosen: B.** The model document proves (README "What a Replica Must Hold") that "no certificate outside
`closure(S)` can change any answer about `S`", where `closure(S)` is `S`, every node with standing over `S`
in the *positive* graph (revocations ignored), every certificate about those nodes, and every revocation
naming one of those certificates. So a query about `S` loads `closure(S)` from SQL and hands it to
`keyline_memory::State`, the reference evaluator, which therefore answers exactly as it would over the
whole set. Nothing is re-implemented but the positive reach (monotone, existence only), and that is what
`closure` returns, so the differential checks it directly.

How the closure is fetched (the `Closure` loader):

1. `T = {S}`. Fetch every delegation whose `subject` is in `T` (index `keyline_delegations_by_subject_audience`,
   prefix `subject`) and the revocations naming them (index `keyline_revocations_by_target`).
2. Compute `R(n)` for every `n` in `T` from the fetched edges, as the threshold program's stratum 1 at its
   lowest level (positive existence): `x in R(n)` iff some delegation `iss -> x` about `Sub` with
   `Sub in {n} u R(n)` and `iss in {Sub} u R(Sub)`.
3. `T' = {S} u R(S)`. If `T'` has nodes not yet fetched, fetch them and repeat; else stop.

Each round is two statements (delegations, then revocations), bound in chunks. Rounds are derivation depth
(single digits on real graphs). The query is read through one `begin_read` snapshot, so every round and the
version row agree.

### Obligations of evaluation-notes section 7

- **1 and 2** (contexts keyed by exclusion-set signature; inert revocations exit early) live inside
  `State`; B runs it unchanged.
- **3** (demand the subject-side feed first; never explore role internals before the role's standing over
  the queried subject is established): a node enters `T` only when it is *grounded* in `R(S)`, that is,
  reached through an edge whose issuer already has standing. An attacker's self-grounded ladder that is
  only *named* (gift certificate: `attacker -> victim`, subject = ladder bottom, attacker without
  standing over the queried subject) contributes the gift's row and nothing else: the ladder bottom is not
  in `R(S)`, so its rows are never fetched. Outsider structure rooted at their own keys is never fetched
  (Tier 0). The residual is the section 7 floor: a member with standing can inflate the closure of the
  documents they belong to, attributably, and the cost follows liveness only for stratum 2 (the positive
  closure includes ex-members, as the model requires, since coverage is computed on the positive graph).
- Memoizing failed demands per set state: not needed, since a failed demand costs nothing here (a node
  that does not ground is never fetched).

### Costs, honestly

- A point query costs O(closure(S)) rows and an in-memory evaluation of that closure, not O(route). The
  README argues closure is near the floor for exact answers; narrower is approximate in the fail-open
  direction. What is gained: unrelated documents cost nothing, cold start is not O(set), there is no
  whole-set memory, and every process reads the same rows.
- `members(S)` is Omega(output) regardless.
- The signature check on read ("a rebuild verifies again", `Error::Stored`) now happens per closure row
  per query. A **verification memo** keeps it affordable: a bounded map from `(kind, digest)` to the
  verified certificate and the exact row bytes it was verified from. A row is trusted without
  re-verification only if its bytes equal the memo's. It is content-addressed and checked against the row
  itself, so it needs no version; it is a cache of a pure function. It is bounded and cleared when full.
- Whole-set operations stay whole-set by their definition: `certificates()` (archives) and `digest()`
  (`set_digest` over every id). Follow-up if `digest` becomes hot: an incrementally maintained set
  accumulator in `keyline_meta`.

### What we deliberately did not build (yet)

A materialized, append-only `keyline_reach(subject, node)` table (stratum 1 only). It is monotone ("never
delete"), so it needs no retraction machinery, and would turn the round loop into one indexed range scan.
It is the first thing to add if closure loading rounds show up in profiles; the loader's `R` computation
is exactly its definition. Stratum 2 stays demand-driven.

## 3. Versioning

Unchanged in spirit and stronger in mechanism: `keyline_meta.version` moves by one in every transaction
that adds a certificate; every query opens a read snapshot, reads the version first, loads its closure
through that snapshot, and returns the version it read with the answer. There is no cache that has to be
brought to a version. A rollback leaves no row and no bump. `insert_in(tx, batch)` / `Staged` /
`committed()` keep their roles: `committed` only warms the verification memo with the batch just verified
(skipping it is safe and only slower).

## 4. Revocation cascades

There is nothing to cascade at insert time: a revocation is a row. Its effect (inert, partial, total) is
decided by the evaluator at the next query over a closure that contains its target. This also gives the
section "Insert" property of the contract for free: out-of-order certificates are stored and contribute
nothing until their target arrives, so no pending state exists in Keyline.

## 5. Schema (Keyline), unchanged tables

`keyline_meta(id, version)` and `keyline_certificates` as in `001_init.sql`: `(kind, digest)` primary
key (`WITHOUT ROWID`), `issuer/audience/subject/power/citation/revoke/retain` copied beside `signed`,
partial indexes `(subject, audience) WHERE kind = 0` and `(revoke, issuer) WHERE kind = 1`. The closure
loader needs no new index: both of its statements are index searches (`suite/plans.rs`).
New statement: `delegation_subject(digest)`, for `is_live` and `conveyed_power`, which are rooted at the
delegation's own subject.

## 6. The rest of keyhive's state: tables to back `KeyhiveStore`

Not implemented here (a follow-up crate over the same `SqliteDb`, composing in one transaction with
`keyline_sqlite`'s `insert_in`). The trait is `origin/keyline/hive-store:keyhive_core/src/store.rs`;
tables follow it method by method. Conventions: ids are 32-byte `BLOB`s; signed values are stored as
their frame bytes and re-verified on decode, as in Keyline; tables prefixed `kh_`.

| Table | Key | Columns | Backs | Notes |
|---|---|---|---|---|
| `kh_key_pair` | `(public_key)` | `secret_ct, nonce, dek_id, dek_version` | `load_key_pairs`, `put_key_pairs` | Secret, global, small. Encrypted per row under a DEK; see below. The trait's "set again changes nothing" is `ON CONFLICT DO NOTHING`. |
| `kh_prekey_op` | `(individual_id, digest)` | `signed` | `load_prekey_ops`, `append_prekey_ops`, `load_prekey_ops_by_digest`, `list_prekey_op_digests`, `list_individuals` | The PK serves by-digest and per-individual loads; `list_individuals` is `SELECT DISTINCT individual_id` over the PK. |
| `kh_cgka_op` | `(doc_id, digest)` | `signed` | `load_cgka_ops`, `append_cgka_ops`, `load_cgka_ops_by_digest`, `list_documents` | Per-doc: a document never needs another's ops. |
| `kh_cgka_op_pred` | `(doc_id, digest, predecessor)` + index `(doc_id, predecessor)` | none | `list_cgka_op_index` | The dependency graph without the operations. Visibility questions are **linear** recursion over it, so a plain `WITH RECURSIVE` expresses "every op reachable from a peer's heads" (unlike Keyline's stratum 1). Index on `(doc_id, predecessor)` answers "who names this op". |
| `kh_doc` | `(doc_id)` | `content_heads BLOB` (canonical list encoding) | `load_content_heads`, `put_content_heads` | Replaced, not appended. Document membership is not a table: `list_documents` reads `kh_cgka_op`. `content_state` is vestigial upstream and gets no column. |
| `kh_content_key` | `(doc_id, content_ref)` | `key_ct, nonce, dek_id, dek_version` | `get_cached_content_key`, `cache_content_key` | The trait calls it a cache, but a miss on the write path is `NoKeyForPredecessor` and, for content of an epoch before this replica's access, the cache is the only copy. So it is **durable in practice**: never evicted by a background policy, only by explicit forgetting. Written in the same transaction as the ciphertext and CGKA op it belongs to. The one per-write unbounded secret set: its growth is the sizing input for the whole store. |
| `kh_pending_event` | `(digest)` | `bytes` | `load_pending_events`, `put_pending_events`, `drop_pending_events` | Unverified; re-verified on load. Ops deferred at load exist only here, so dropping the table loses them until redelivery. Keyline needs none of this (section 4). |

**Key pairs and DEKs.** Secret columns use the scheme big_repo already has (`big_repo_deks`,
`big_repo_secret_blobs`): AES-GCM-style AEAD under a DEK, the DEK wrapped by a `secrets_rs` KEK whose
source is outside the database (keyring, KMS). One `kh_dek(dek_id, dek_version, wrapped_dek, kek_version,
algorithm)` table; the secret tables carry `(dek_id, dek_version)` so a KEK or DEK rotation is
incremental. The AEAD associated data binds the row key (table, public key or `(doc, ref)`), so a
ciphertext cannot be moved between rows. With "no files, one database", the KEK is the only thing that
must live elsewhere.

**Transactions.** Every write method takes the caller's transaction (`&impl Executor`), the same shape as
`SqliteKeyline::insert_in`. A prepared write's persist step (ciphertext row in the app's table, content
key, CGKA op and predecessors, leaf key pair, listener-reported ops) plus any grants (`insert_in`) is one
commit; keyhive applies in memory after commit (`Preliminary` in the trait says what must be persisted in
any committing transaction).

**Anticipating a larger upstream trait.** Upstream's Keyline trait is narrow and has no tx; `KeyhiveStore`
already is the first widening. We expect a combined hive store trait later. So each store exposes `_in`
forms over the caller's snapshot or transaction, never only the trait methods, and the SQLite crates stay
generic over `SqliteDb`; a composite `HiveSqlite` that implements both traits over one handle is then a
type, not a refactor. For Keyline this branch adds `SqliteKeyline::closure_in(snapshot, subject)`
returning an `Evaluated` whose methods are the evaluated queries, so a keyhive operation (a CGKA
authorization check, say) can read ops and Keyline answers in the same snapshot.

## 7. Schema decisions that need an operator answer

1. Is `scope_id` (one database, several hives) wanted? Both stores here assume one hive per database.
2. Whether `digest()` should be maintained incrementally (section 2).
3. Whether to materialize `keyline_reach` now (section 2) for relay deployments holding many documents.
4. Encrypt `kh_cgka_op`'s secrets? They are already encrypted to leaf keys inside the ops; only
   `kh_key_pair` and `kh_content_key` hold plaintext secrets, so only those get the DEK.
