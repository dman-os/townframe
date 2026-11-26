# Hive store: Keyline from SQL on demand, and the rest of keyhive's state beside it

Owner: the automata lane (keyline-eval).
Scope: `src/keyline_sqlite` (implemented) and the non-Keyline keyhive tables (proposal only).
Updated: 2026-11-26.
Status: design for `keyline_sqlite` (implemented on this branch, revised to the materialized shape) and for the non-Keyline tables (not
implemented; sketched so the Keyline tables and a follow-up crate share one database and one
transaction). Sources: keyhive fork `origin/townframe-changes` (`design/keyline/implementation.md`,
`evaluation-notes.md` section 7, `keyline_memory::state`), `origin/keyline/hive-store` at `4053d96`
(`keyhive_core/src/store.rs`, the `KeyhiveStore` trait), and the kh-archive report.

## 1. The problem, and how the design changed

The first version of `keyline_sqlite` stored certificates in SQL but answered every evaluated query
from a `keyline_memory::State` over the **whole** set: a cache miss re-read and re-verified every row,
and the cache was one set per process. A second attempt loaded `closure(S)` into the same reference
evaluator per query. Both run the evaluator in Rust over rows pulled out of the database, which is not
the shape the Keyline design recommends: `design/keyline/evaluation-notes.md` section 5 ("An SQL
Sketch") and section 6 ("The Split That Matters") say to **materialize the monotone stratum and
evaluate the negated stratum only where it is disputed**. The crate now does that. The closure-in-Rust
evaluation survives in exactly one place, `members_for_subset`, whose answer is defined over an
arbitrary subset and so cannot come from tables derived from the whole set.

## 2. The design

### 2.1 Stratum 1 is a table, extended at insert

`keyline_reach(s, n, lvl, epoch)` is the threshold form of `reaches`: a row means "node `n` has standing
of at least `lvl` over subject `s`, every revocation ignored". Four boolean passes, no aggregates. A
node's standing over itself is implicit (no row), which costs four join cases instead of four stored
rows per node; `s <> n` is a table check. It is append-only: nothing is ever deleted, so extending it is
incremental by construction ("incremental for a monotone relation is just never delete").

`insert_in` extends it **inside the caller's transaction**, after the certificate rows and before the
version bump returns:

1. The batch's columns are staged in `keyline_new` (one multi-row statement per ~4000 certificates).
2. **Round zero** (`reach_seed.sql`): one statement joins the batch's delegations against the stored
   reach. The rule has two recursive premises, so each of its cases is one join over a stored relation:
   `s = Sub` (feed 1 is the subject itself) or `reach(L, s, Sub)`; `Iss = Sub` or `reach(L, Sub, Iss)`.
3. **Round k** (`reach_round.sql`): one statement, three cases, each joining only the previous round's
   rows (`epoch = :prev`, found through `keyline_reach_by_epoch`) against the delegations and the
   stored reach: feed 1 new; feed 2 new with feed 1 the subject itself; feed 2 new with feed 1 stored.
4. The application loop runs rounds until one inserts nothing. A round's number is a counter in
   `keyline_meta.epoch` that never repeats, so "the previous round's rows" is an index range, not a
   temp table, and the same statements would run unchanged inside a PL/pgSQL `LOOP`.

Late binding falls out: a delegation inserted before its issuer has standing derives nothing, and the
batch that gives the issuer standing derives it, because the new reach rows are the delta of the next
round and join against *all* delegations. Insertion order never matters.

### 2.2 Admin reach and coverage are joins over it

`admin_reach(k)` is `SELECT s FROM keyline_reach WHERE n = k AND lvl = 3` (index
`keyline_reach_by_target`) plus `k`; coverage is the join of a delegation's revocations with their
issuers' admin reach. No stored view: stratum 2's refresh computes what it needs.

### 2.3 Which subjects need stratum 2

A subject's live set can differ from its positive reach only if its closure holds a delegation some
revocation names. At insert, in the same transaction, `keyline_dsubject` records the subjects of
delegations named by a revocation (whichever of the two arrives first), and `keyline_disputed` the
subjects that stand in the positive graph over such a subject or are one (two statements: from new
dispute marks over the stored reach; from new reach rows over the stored marks). Both are monotone.
The flag is conservative: a revocation that covers nothing (an outsider's) still marks the subject, so
it pays the slow path without changing the answer. An undisputed subject answers entirely from stratum 1.

### 2.4 Stratum 2, per disputed subject

For a disputed subject `S` the live relation `keyline_live(s, ctx, n, lvl)` is computed by SQL rounds:

- **Roots**: the nodes in `S`'s closure that are the subject of some delegation. Each hop is judged
  rooted at its own subject (the daisy-chain rule), so the rounds read rows of every root in the
  closure, and the roots are closed under what the rounds read.
- **Contexts, keyed by the canonical excluded-set signature**: for each delegation about a root that a
  revocation names, the covered set is the union of the third-party revokers' admin reach; a revoker
  that is the delegation's issuer or audience kills it outright (`total`). Delegations with the same
  covered set share one context (`keyline_ctx`, signature = the sorted node ids concatenated), so a
  one-key spree of `k` revocations costs one context: obligation 1, k^3 to k^2. A delegation whose
  covered set is empty gets context 0 and no row: obligation 2, an inert dispute costs a lookup.
- **Rounds** (`live_seed.sql`, `live_round.sql`): seeded by root edges in every context in use, then five
  semi-naive branches per round (feed 1, feed 2 with feed 1 reflexive or stored, the hop's own-context
  premise with feed 1 reflexive or stored), until a round inserts nothing. A hop conducts `s -> aud` in
  context X when feed 1 and feed 2 hold in X, it is live in its *own* context rooted at its own subject,
  it is not killed outright, and X does not exclude the audience.

Uncovered delegations take the fast path: they have no `keyline_dispute` row, context 0, and their own
check is satisfied by the same facts.

**Cached per subject, validated by version.** The rows are stored with the version they were computed
at (`keyline_live_at`) and answer a query only at that version; otherwise the query computes them in a
write transaction (a cache fill any process may do) and answers there. I chose caching over recomputing
because a read snapshot cannot write (`sqlite_rs` rejects non-read statements on the read path), so
per-query recomputation would need a write transaction per question, and measurement says a refresh
costs 26 statements and rows read in proportion to the disputes in the closure (29 rows for 20 disputed
delegations, 209 for 200) while a cached question costs 4 statements and 4 rows. The cost of the
coarse validity: any insert, to any subject, invalidates every disputed subject's rows. Per-subject
validity (stamping with the version of the last insert that touched the closure) is possible and not
built.

### 2.5 Point queries

| Question | Undisputed subject | Disputed subject (cache current) |
|---|---|---|
| `effective_power(s, a)` | `is_disputed`, `reach_standing`: 3 statements, 3 rows | + `live_stamp`, `live_standing`: 4, 4 |
| `positive_power(s, a)` | `reach_standing` | same |
| `members(s)` | `reach_members`: one range read, rows = the answer (x levels) | `live_members`: same |
| `is_live(h)` / `conveyed_power(h)` | `delegation_row` + `reach_standing` | + `dispute_of`, `live_standing` |
| `closure(s)` | one statement over the reach and the delegation indexes | same |

`members` returns `max(lvl)` per node by a group over the stored threshold rows: up to four rows per
member. That is O(output), not the O(route) §6 aims for a point query, and a point query is O(1).

### 2.6 Obligations of evaluation-notes section 7

- **Grounding, never optimistic seeding.** Both fixpoints are least fixed points from root edges.
  `suite/demand.rs` plants an optimistic seed (the grounding condition removed) and the gift test fails.
- **A gift certificate costs its own row.** A key with no standing over `DOC` gifts the top of a ladder
  (10 and 60 rungs): inserting the gift derives **0** facts and `members(DOC)` is unchanged. The ladder's
  own certificates are grounded in the rungs' own graphs, so they *are* derived at insert (k^2/2 facts
  for the club ladder, the semantic floor of section 7) whether or not anyone gifts them. That is the
  "eagerness inverts the cost model" point of section 6: a replica pays for grounded junk at ingest.
- **Obligations 1 and 2**: section 2.4.

## 3. Measured costs

Debug build, mothership, counted at the database wrapper:

| | Statements | Rows |
|---|---|---|
| Insert of 111 / 1101 certificates (star with revocations) | 14 / 14 (2 rounds after round zero) | 204 / 2004 facts derived, about 1.8 per certificate |
| Undisputed point query, 200 / 2000 members | 3 / 3 | 3 / 3 |
| Disputed point query, cache current | 4 / 4 | 4 / 4 |
| First question after an insert on a disputed document | 26 / 26 | 29 / 209 (the disputes in the closure) |
| `members`, 201 / 2001 members | 3 / 3 | 203 / 2003 (about one row per member) |
| Query about a small document beside 50 / 500 unrelated members | 12 / 12 | 16 / 16 |

Differential against `MemoryKeyline`: 64 cases over the conformance pool (8 keys) in 28 s, 20 cases over
16 keys with nesting and revocations in 20 s (the closure-in-Rust version took 46 s and 61 s); the deep
differential also passed at 300 cases (below). Fewer statements per insert, not more: the fixpoint cost
moved from every query to the insert.

## 4. SQLite-only constructs, and what a PL/pgSQL backend needs

- `INDEXED BY keyline_delegations_by_subject_audience` in `subjects.sql` (existing). Nothing in the
  evaluation uses `INDEXED BY`.
- **Planner dependence instead**: without statistics SQLite scans the primary key's `kind` range whenever
  `kind = 0` is spelled out, so the evaluation's statements never mention `kind` for delegations and
  revocations (a subject or target equality implies it) and the indexes are partial on the keyed column
  (`WHERE subject IS NOT NULL`, `WHERE revoke IS NOT NULL`). Scratch-bounded statements are driven by
  `d.subject IN (SELECT s FROM keyline_root)` so the planner probes by index. `suite/plans.rs` fails on
  any scan outside the four small driver tables (levels, batch, roots, contexts). PostgreSQL's planner
  with statistics needs none of this; the statements are valid there as written.
- DDL: `STRICT`, `WITHOUT ROWID`, `INTEGER PRIMARY KEY` (the context id, a rowid alias), the blob
  literal `x''`, `ALTER TABLE ... ADD COLUMN` (portable).
- `INSERT ... SELECT ... WHERE true ON CONFLICT DO NOTHING`: the `WHERE true` is SQLite's parsing
  ambiguity workaround and harmless elsewhere; `ON CONFLICT DO NOTHING` replaced the `INSERT OR IGNORE`
  in `write.rs`, and its inserted-row count is what both engines report.
- Named parameters `:epoch`, `:prev`, in first-appearance order (a translator to `$n` must do the same).
- The round loop is Rust here; in PL/pgSQL it is `LOOP ... GET DIAGNOSTICS ... EXIT WHEN 0` around the
  same static statements. The single-writer assumption (the epoch counter is a hot row in
  `keyline_meta`) holds for both with an advisory lock per hive.

## 5. Where the SQL sketch and the reference differ

The sketch "is not in the repository, and nothing tests it", so I implemented from the threshold program
in `implementation.md`, which the conformance suite's second oracle transcribes, and let the differential
decide. No answer disagreed with the reference once implemented, but the implementation differs from the
sketch's text in these ways (each is answer-preserving, and the planted-bug tests show the parts that
matter):

1. The sketch seeds `reaches(lvl, p, p)` for every principal at every level. The reflexive facts are
   implicit here, and each rule has the join cases for "the subject itself" explicit.
2. The sketch's conduct statement has no `aud <> s`; here `n <> s` is a table check and the statements
   exclude it.
3. Stratum 2 in the sketch is prose. The conditions `not excl(X, Iss)` and `not excl(O, Iss)` of the
   threshold program are dropped where an explicit row already implies them and kept for the reflexive
   case; the audience check (`not excl(X, Aud)`) is the one the deep-revocation scenario depends on.
4. Contexts are materialized for every root in the closure, not per queried subject, because the hop's
   own check reads rows rooted elsewhere.
5. "Disputed" is conservative (any delegation a revocation names), where the sketch says "covered".

## 6. Insert path, as it stands

`insert_in` writes the certificate rows (`ON CONFLICT DO NOTHING`), bumps the version, stages the batch,
runs the stratum 1 rounds and the dispute marking, and clears the staging table, all in the caller's
transaction. A rollback leaves no row, no reach and no version bump. `Staged::extension()` reports rounds
and facts. `committed()` only warms the verification memo, which now serves `members_for_subset` alone.

**What reads no longer verify.** Evaluated answers come from tables derived from certificates verified at
insert, so a row edited outside the backend changes `get`/`closure` bytes but not the derived tables,
and only `members_for_subset` (which loads certificates to evaluate them) re-verifies. An audit that
rebuilds the reach from the certificates is the way to detect or repair such an edit; not built.

## 7. The rest of keyhive's state: tables to back `KeyhiveStore`

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

## 8. Decisions that need an operator answer

1. **Per-subject validity of stratum 2 rows.** Today any insert invalidates every disputed subject's
   live rows (section 2.4). Finer validity needs a version per closure; worth it only if disputed
   subjects are queried between frequent inserts.
2. **Refreshes write.** A question about a disputed subject whose rows are stale computes them in a
   write transaction, so an inert process that only reads takes the single writer for a refresh. The
   alternative is a read-path temp table, which `sqlite_rs` forbids on read snapshots.
3. **Eager cost of grounded junk.** Stratum 1 is derived at insert, so a member's grounded ladder costs
   k^2/2 facts on every replica that holds it, asked about or not (section 2.6). A per-issuer quota at
   sync admission (mitigation 4 of section 7) is the lever; it is not in this crate.
4. **No re-verification on evaluated reads** (section 6). An audit/rebuild function is the remedy.
5. `scope_id`: one hive per database is assumed.
6. Whether the non-Keyline `kh_*` tables become a follow-up crate over the same `SqliteDb` (assumed yes).
