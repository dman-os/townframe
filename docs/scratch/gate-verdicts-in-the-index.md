# Gate verdicts in the datum-set index

Status: proposal
Owner: nodules lane
Updated: 2026-11-25
Scope: `big_cabinet` read path (`doc/read.rs`, `doc/gate.rs`) and the datum-set index (`index/datum_set.rs`). Motivated by node-95's `mirrored()` measurement: every `readDatums` re-runs the read gate on every datum it returns.

## 1. Problem and profile

`DocsService::read` calls `Gate::verify` once per returned datum. Reading the known-drawers list (one datum per known drawer) is therefore linear in the drawer count with a large constant. The gate is necessary on the read path: documents arrive through sync without passing it, so the read check is the only verification (`gate.rs` module comment).

Measured with the node-95 harness (`long_test_mirrored_cost_by_known_drawers`), release build, **mothership-alpha (x86_64)**, `BIG_CABINET_KNOWN=300` (1000 exceeds nextest's 240 s hard timeout in the test's own setup, creating 1000 drawers; the cost is per datum, so the size does not matter). Phase timers are `// INSTR` lines in `gate.rs`, `claim_history.rs` and `read.rs` (commit `9cd8a164`, droppable on its own). Totals cover the five `mirrored()` asks of the median loop (1505 `verify` calls).

| Phase (per `verify`, microseconds) | us | share |
| --- | ---: | ---: |
| **gate total** | **416** | 100% |
| `reconstruct_current_claims` | 294 | 71% |
| of which: Ed25519 verify of the claim signature (`SignedDatumClaim::verify`) | 64 | 15% |
| of which: signature entries at the introduction (`entries`) | 46 | 11% |
| of which: actor binding (public key, URL, certificate) at the introduction | 41 | 10% |
| of which: aliases at the introduction | 42 | 10% |
| of which: signature entries at H (`entries(current)`) | 37 | 9% |
| of which: predecessor ids | 21 | 5% |
| of which: datum value at the introduction | 17 | 4% |
| `claim_status` | 106 | 25% |
| of which: attribution certificate verify (`Attribution::verify`) | 105 | 25% |
| value at H (`native` at the current heads, single-claim fast path skips the signed one) | 14.5 | 3.5% |
| (outside the gate) hydrating the JSON value for the reply | 10.5 | |
| (outside the gate) `datum_version` | 0.7 | |

Wall per `mirrored()` ask was 128 ms for 300 known drawers (427 us per known drawer); the gate is 125 ms of it. The gate is the whole read cost. Hydration is 2.5% of the gate's cost and is not the problem.

Where it goes: **about 41% is two signature verifications** (the claim signature and the attribution certificate, 169 us), and **about 53% is historical Automerge reads** at each claim's introduction heads and at H (219 us: entries, actor binding, aliases, predecessors, value, current entries, value at H). Neither is a single dominant term. Dropping the certificate verify (which repeats the same actor's certificate for every datum) would save at most 25%; it cannot reach a cost anyone wants to pay per read. The only way to stop paying both is to not re-derive the verdict.

The earlier aarch64 mothership numbers (node-95) are not comparable with these.

## 2. The datum-set index today

`DatumSetStore` (`index/datum_set.rs`) is the only index worker. It follows the repo's change stream, one delta per document (`Work { doc_id, state: DocState { heads, epoch, cursor } }`), and per delta:

1. `prepare` loads the stored `DocState` and the document (`get_doc_for_projection`), and defers when the document is missing dependencies of the delta's heads (`get_missing_deps`).
2. `project` runs inside `handle.with_document`. `DocView::project` computes the **touched** keys: an incremental pass over `get_changes(old_heads)` decoding each op's path and collecting `datums/<key>` and `datums/dmeta/datums/<key>`; a full pass (all keys) when there are no prior heads, the epoch changed, or an op's path is unclassifiable. `DocView::slot` reads each touched slot's **unverified** signature-entry heads and membership (`Present` or `Removed`).
3. For tags whose schema declares references only, `edges_from` runs `Gate::verify` and then `hydrate_value` to extract the edges. So the index already pays one gate run per such datum at materialization.
4. `settle` writes, in one transaction: the entries (`big_cabinet_datum_set_entries`: `doc_id, datum_key, datum_tag, doc_heads, datum_heads, clock`), the edges, and the `DocState` (`big_cabinet_datum_set_docs`).

What the existing rows hold, and what they do not: `datum_heads` is the *unverified* signature heads (`signature_heads` shape), and `doc_heads` is the heads of the projection that last **touched** that key. An untouched key keeps its older `doc_heads` by design. The authoritative "the index has processed everything up to heads X" mark is the document's `DocState.heads`, written atomically with the rows.

## 3. Proposal

### 3.1 What is stored

Add a verdict to each `Present` entry row (migration `003_verdicts.sql`), computed by `Gate::verify` for every touched `Present` key during `project`:

- `verdict` (INTEGER, NULL = not computed): `Accepted`, `Rejected`, `Pending`.
- `verdict_authors` (BLOB, JSON): the `DatumAuthor` list of an `Accepted` verdict.
- `verdict_reason` (TEXT): the reason of `Rejected` and `Pending`.

The verified datum heads need no new column. For `Accepted` and `Pending` they equal the stored signature heads (every current entry contributes its introduction to the gate's datum heads, valid or pending), and for `Rejected` the reader reports empty heads, as the gate does. The verdict's `native` bytes are not stored: the reply's value is hydrated from the document at H either way (see 6).

A new constant `GATE_VERSION` is folded into `DATUM_SET_ID` (`big-cabinet-datum-set-v2` becomes `...-v3-gate<N>`), so any change to gate logic, claim reconstruction, attribution or budgets that bumps it makes the index rebuild from the repo instead of trusting verdicts the new code might compute differently.

`Removed` rows are deleted today; a removed or absent slot keeps taking the full path (its gate run is cheap: no value to compare, but it still reconstructs claims; open question 4).

### 3.2 When it is trusted

`readDatums` fetches, before entering `with_document` and in one query, the document's `DocState.heads` and the verdict columns of the requested keys (all `Present` keys when `datums` is omitted). Inside the document closure, with the read's resolved heads H (sorted):

- **Trust** a row when `DocState.heads == H` and its verdict is `Accepted` or `Rejected`. The gate is skipped; `datum_version` uses the stored heads, the authors come from the row.
- **Verify in full** (today's path) for: any key with no row or NULL verdict, a `Pending` row, any H that is not the index's heads for that document (a historical `At::Heads`, or a current read the index has not caught up with yet, for example right after a write), and a document the index does not have (`datum_set` is `None` in several tests).

The lookup compares against the state fetched in the same query, so a stale fetch is a mismatch, never a wrong trust: the state and rows are one snapshot, and H is checked against that snapshot's heads.

### 3.3 Pending

`Pending` is stored as such and **never trusted**: a read of a `Pending` row always re-runs the gate. The brief's wording ("re-verified when the missing certificate arrives") needs a correction from the code: the gate does not produce `Pending` for a missing certificate. `gate.rs` says a missing certificate or document metadata is *malformed*, and `claim_history.rs` maps only `ResourceRefused` (a `HistoryBudget` limit) and `Unsupported` (a claim or certificate format this build does not know) to `Pending`. Both are functions of the document at H and the **code**, not of data that can arrive later: claims are reconstructed at their own introduction heads, so a later change cannot add the missing piece to an earlier introduction, and the index defers a document with missing dependencies, so it never settles a causally incomplete view.

So the index "learns" a `Pending` key is resolvable in exactly two ways, neither new:
- the key is touched by a new change (the incremental pass picks it up and `project` re-runs the gate on it);
- the code changes (a larger budget, a newly supported format), which is the `GATE_VERSION` bump and a rebuild.

Because reads re-verify `Pending` rows anyway, a `Pending` datum is never stuck behind a stale stored verdict. The stored `Pending` row is bookkeeping: it lets the read path skip the lookup for the keys that matter. If a future gate version does start producing `Pending` for data that can arrive (for example a certificate delivered out of band), that arrival event is the retry trigger and must be designed with it; the present rule (never final) is what keeps that safe.

## 4. Invariants

- **Purity.** The verdict of `key` at heads H is a function of: the signature-entry list of `key` at H; each current claim reconstructed at its introduction heads (`resulting_heads = [introduction]`, immutable, including the actor binding and certificate read there); the value of `key` at the datum heads (immutable, derived from introductions) and at H; and `HistoryBudget` plus the code. Nothing outside `key` is read ("Checks are datum-local" in `gate.rs`).
- **What changes it.** Only a change that touches `datums/<key>` or `datums/dmeta/datums/<key>`, which is exactly the touched set `DocView::project` already computes to maintain the entries rows. Changes to aliases, actors or timestamps do not change a key's verdict (the existing projection already relies on this for `datum_heads`), because the gate reads those only at historical introductions.
- **Invalidation.** A row is rewritten when its key is touched and left alone otherwise; the doc-level `DocState.heads` moves in the same transaction. A read at any heads other than `DocState.heads` never consults the row. There is no time-based or count-based expiry.
- **Rebuildability.** The index is local state derived from the repo. Dropping the entries and doc rows (or bumping `GATE_VERSION`) re-projects every document from scratch; until a document is re-projected its rows are absent, which means "verify in full". A rebuild only costs time, never correctness. It is also why this is not a cache with a cold start: the rows are written once at ingest and survive restart.

## 5. Hydration

Storing verdicts adds **gate work at materialization, and no extra hydration**. The gate's value reads use `NativeValue::read_at_heads` (canonical encoding at heads), not the schema `hydrate_value` that builds the reply JSON. The reply still hydrates from the document at H exactly as today (10 us per datum on alpha), which the read path needs regardless. `edges_from` already runs the gate and the hydration for reference-declaring tags; those datums should take the gate run once and share the verdict with the new column instead of verifying twice.

## 6. Risks

- **Verdict trusted for heads it was not computed at.** The only defence is the equality of H with the snapshot's `DocState.heads` (3.2) together with the touched-set argument (4). The touched-set pass is the same logic the membership and `datum_heads` columns already rely on, and its soundness is now load-bearing for a security decision, not just for freshness. Its documented weak points (a full pass when an op's path is unclassifiable) fail towards recomputing, which is the safe direction. A test must pin the property directly: for a random schedule of signed, unsigned and removing writes, the stored verdict equals a fresh `Gate::verify` at `DocState.heads`.
- **A forged row in local SQLite.** The threat model for local state is that whoever can write the index database can already change membership, grants and the keyhive store beside it, so a forged `Accepted` verdict is not a new capability class. Two cheap hardenings, neither in the first cut unless the operator wants them: (a) the read path recomputes `signature_heads` at H for trusted keys and compares with the row's `datum_heads`, which catches stale, copied or index-bug rows at the cost of one cheap listing-grade read per key (no crypto); (b) an HMAC of `(doc_id, key, heads, verdict, authors)` under a node-local secret. Neither stops a row for a datum whose *value* was changed by an unsigned edit with the signature entries untouched; only the value comparison catches that, and that comparison is the 3.5% term we would be skipping. Open question 2.
- **Cost moves to ingest.** The index worker runs the gate for every touched `Present` key, inside `handle.with_document`, which holds the document. A first projection of a 1000-datum document costs about 0.4 s of that hold (alpha, release). Steady state is incremental, so only touched keys pay. This is the same trade as the `edges_from` precedent. Mitigations to decide: verify in chunks that yield the document between chunks, or run the gate on a `fork` outside the lock.
- **Lag.** After a write, reads miss until the index settles, then run the full path; a document under constant writes reads at today's cost, never worse.
- **Coupling to the gate.** The stored verdict encodes the gate's semantics at write time; `GATE_VERSION` must be bumped with any behavioural change to `gate.rs`, `claim_history.rs` or `Attribution::verify`. A test should fail if the gate's output changes while the constant does not (a golden verdict fixture).

## 7. Alternatives considered

- **Cut the dominant cost directly.** The profile has no single dominant term, but the biggest one is the attribution certificate verification (25%), repeated for the same actor on every datum. A per-call map of already-verified certificates would remove most of it, and batch Ed25519 verification could roughly halve the claim-signature term. Together that is plausibly a 30-40% cut, and the rest (historical reads, 53%) stays. It keeps a verdict per read and cannot get a 1000-datum read under about 250 ms. Worth doing later for ingest and for the full-verify path, which the index will still use.
- **An LRU of verdicts keyed by (doc, key, heads).** Rejected by the operator: cold exactly after restart, which is when the many-datum reads happen, and it hides the cost instead of paying it once.
- **Trust at the row's own `doc_heads` rather than the document's `DocState.heads`.** Would let reads at older-but-equivalent heads hit, but a key's row heads lag its document by design, so the comparison would need an ancestry and untouched-between check at read time. Not worth it for the first cut.
- **Reuse the verdict on the write path** (`commit_write` verifies the base for every planned key, `list_datums` with authors, merge). The same trusted lookup would serve them, but writes already pay the gate once and the cost is per planned key, not per listed key. Later.

## 8. Open design questions

1. **Document lock during first projection.** Chunking the gate inside `with_document`, running it on a fork outside the lock, or accepting the hold (about 0.4 s per 1000 datums)?
2. **Forged-row hardening.** Baseline "local SQLite is trusted", or add the signature-heads consistency check (cheap) and/or an HMAC on every trusted read?
3. **Trust key.** Compare H against `DocState.heads` (simple, proposed) or add an ancestry-and-untouched check so reads slightly behind the index still hit?
4. **Removed and absent slots.** Store verdicts for `Removed` rows too (needs the delete-on-remove behaviour changed), or leave them on the full path?
5. **Rejected rows.** Trusting `Rejected` skips the gate for a forged datum, which is what we want, but it also means a gate bug becomes sticky until a `GATE_VERSION` bump. Acceptable?
6. **Gate versioning.** Fold `GATE_VERSION` into `DATUM_SET_ID` (drops and rebuilds the whole index on every bump) or version only the verdict columns and re-verify lazily?
