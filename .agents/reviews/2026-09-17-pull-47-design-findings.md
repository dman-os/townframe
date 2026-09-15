# Pull #47 design findings — FDRs 001–004, ADRs 010–013

**What this is.** The ledger for a design review of the PR #47 documents, done before we
write more code. One external LLM reviewed the docs; the operator answered several
points inline; this file is the verified result: what is a real contradiction, what is a
real hole, what is misinformed, and what is already decided somewhere else.

**Not a spec.** Each item names the document that *owns* the fix. Nothing here should be
duplicated into more than one doc (the duplication is already producing drift).

**Status vocabulary**

| status | meaning |
|---|---|
| `CONTRADICTION` | two places in our own docs say different things; one is stale |
| `HOLE` | nothing says anything, and correctness depends on an answer |
| `WORDING` | the mechanism is right, the words are wrong or too strong |
| `OPEN` | already an open question in the doc; needs an answer, not an addition |
| `MISINFORMED` | the review's premise does not match the docs or the code |
| `DECIDED` | settled earlier; the review did not have the decision |

**Verification basis** (checked in this pass, file:line):

- `src/pauperfuse/bridge.rs` — `reconcile`, `Outcome`, `transfer`, module docs.
- `src/pauperfuse/backend.rs` — `BackendId`, `Capabilities`, `Accepted`, `Report`.
- `src/pauperfuse/fs.rs` — `read`, `materialize`, `accept`, `may_remove`, `remove`,
  `write_file_atomic`, `scratch_path`; grep for an instability check (none).
- `docs/fdrs/001-dpaths.md` — syntax rules, Rules 1–3, conflict prose, Binding stability,
  Appendix A.
- `docs/fdrs/003-vc-primitives.md` — status/commit/diff table, redaction, history grouping.
- `docs/fdrs/004-workspace-cli.md` — ambient ingest, adopt, import, auto-commit, naming.
- `docs/adrs/010-vtree-store.md` — §2.5 aliasing, §3.3 reps, §4 pseudocode, OQ list.
- `docs/adrs/011-checkouts-reconciliation.md` — expectations, rep-healing paragraph.
- `docs/adrs/012-lenses.md` — trait sketch, conflict contract, OQ list.
- The design session's own user turns (`2026-09-15T14-09-49-068Z`, Aug 30 – Sep 15), for
  intent — cited below as **[intent]** with the message number.

---

## A. Contradictions inside our own documents

### A1. Rule 1 vs Binding stability vs Appendix A case 10 — `CONTRADICTION`
FDR 001 Rule 1 (§5) resolves two claimants of the same dpath by making the dpath a
**directory** and putting both under it: `/inbox/hello.md/<name-a>.<ext>` and
`…/<name-b>.<ext>` (FDR 001 §5, Appendix A case 2).
FDR 001 §"Binding stability" says the opposite principle: *"A materialized binding is
never rewritten… later arrivals must NOT rename it"* (FDR 001:223–227).
Appendix A case 10 says the opposite of Rule 1: *"binding is sticky: A keeps `/x.png`; B
lands at `/x~<id-b>.png`"* (FDR 001:458).

So case 2 and case 10 cannot both be the rule for the same situation, and Rule 1
actively violates Binding stability: under Rule 1 the incumbent is renamed out of the
literal path the moment a second claimant arrives.

Also load-bearing: §5 claims the procedure is **order-independent**, but sticky bindings
make the *winner* order-dependent by design. Uniqueness/totality is order-independent;
which claimant keeps the clean name is first-come. Both claims need scoping, in one
sentence.

**Recommendation.** Retire the file-vs-file dir spill. One principle:
> An already-bound claimant keeps its literal path; new claimants derive a name
> (`~<id>` sibling). The `.d` machinery stays for the two cases that genuinely cannot
> share a position: a file with children under it (Rule 2), and two *directories*
> competing for one path (case 8, where a directory cannot be a file).

That matches **[intent]** msg 7 ("a file that has a path today might not have it
tomorrow… first come first served serves the interop usecase better") and makes Rule 1,
Rule 2, Binding stability and case 10 one story. `Q1` (the naming scheme, `~<id>` vs
`<name>.<ext>`) stays open, but the *shape* stops being three-way.

*Owner: FDR 001 §5 + Appendix A.*

### A2. FDR 001 renders conflict markers; ADR 011 forbids them — `CONTRADICTION`
FDR 001:262–266: *"The materializer renders conflicted state in-place (conflict markers
for text lenses, e.g. `<<<`/`>>>` blocks)"*.
ADR 011:123: *"**No on-disk conflict markers, ever.**"* ADR 012:163 says
`render_conflict` does not exist and lenses "render last-good states".

ADR 011 + ADR 012 are the decision; FDR 001 is stale. The operator already called this
("the FDR is stale").

*Owner: FDR 001 §6. Replace with: a bounce is an opinion about the data, the disk keeps
the last-good render, the CLI shows a stdout diff (`db diff` / conflict viewer).*

### A3. Workspace-wide `--at` — `WORDING`
ADR 010:485: *"There is **no workspace-wide "checkout at point N"** and we should not
pretend otherwise."* FDR 003:91–93 exposes `db fork --at <version>` and
`db checkout --at <version>` ("heads-pinned views").

Not a real conflict if `--at` pins *each selected doc at its own version*, but the docs
must say so: a pin is a per-doc heads vector, not a snapshot of the workspace. Also note
FDR 003:157 already says fork-at-version clones "at those heads" — plural, per doc.

*Owner: FDR 003 §3/§6 one sentence; ADR 010 keeps its sentence.*

### A4. "The last change id … across all touched docs" — `CONTRADICTION`
FDR 004:166–167: a commit "names the last change id of the local ingest **across all
touched docs**". There is no such thing: independent Automerge docs have independent
change ids. FDR 004's own naming model already has the right shape —
`(branch, from_heads…to_heads) → name` (FDR 004:175).

**Recommendation.** Say frontier map everywhere: a checkpoint is
`{ doc → (from_heads, to_heads) }`. Which raises the real question the review correctly
identified:

> **What correlates one user-level operation across N docs?**

**[intent]** msg on this turn: per-doc names, plus either a correlation token or a
history facet, and note that Automerge can name a range by emitting an empty commit.
Options to weigh (one decision, three parts):
1. *immutable, native*: each doc gets an ordinary change carrying a shared correlation id
   in its message — survives replication, costs no extra change;
2. *mutable, user-facing*: the checkpoint name stays an annotation over the range
   (already decided, FDR 004 §6);
3. *daybook-native*: a history facet in the node mapping
   `activity → { doc → heads }` — fits "backends own their data", and is the only option
   that can be read without scanning every doc's change metadata.
An empty commit to close an unnamed range is a real capability but buys nothing a
correlation id does not; keep it in the back pocket.

*Owner: FDR 003 §5 (history grouping) states it once; FDR 004 §6 points at it.*

### A5. Adopt "assigns dpaths" without creating docs — `WORDING`
FDR 004 §3 step 2: *"assign dpaths from origin paths"*; step 3: *"track every file in the
pauperfuse checkout tree **without creating any docs**"*. A dpath assignment is a facet
inside a doc (FDR 001), so step 2 has nowhere to live as written. Likewise "daybook object
identity is created behind the scenes" (FDR 004:86–87) reads as contradicting "no
documents are created".

**[intent]** the operator's own answer: *"they get paths in the vtree for the fs backend,
they won't be dpaths until they get into the daybook backend."*

**Recommendation.** Vocabulary: adopt records **candidate paths** (a plan, in `.dtree`);
`db import` turns a candidate into a **dpath claim** in a doc. Say the plan is
"what the conversion *would* do", never "assigned".

*Owner: FDR 004 §3.*

### A6. `/trash` described as reserved — `WORDING` (but load-bearing)
FDR 001:84: *"**No reserved namespaces in daybook.** Dpath labels are not restricted."*
FDR 004 §4: *"the reserved `/trash/` default-exclusion"*.

**[intent]** the operator: *"removal going under trash is still a dpath claim, it's just
that most checkouts wouldn't include the trash set in their dpath query. It's not a
reserved path, it's like the Recycle Bin."*

**Recommendation.** Three distinct levels, spelled out once (FDR 001 §4):
1. **daybook**: nothing is reserved; `/trash/...` is an ordinary claim;
2. **materialization surfaces** (`/by-id`, the checkout metadata dir): not reserved
   either — they win their literal real path by collision rules (already correct,
   FDR 001:289–293);
3. **checkout selection**: a *default-exclusion clause* over `/trash/**`, owned by FDR
   003 §8 and consumed by FDR 004 §4. A checkout that asks for `/trash` materializes it.
Rename "reserved clause" → "default-exclusion clause" in FDR 004 §4.

*Owner: FDR 001 §4 (statement), FDR 004 §4 (wording).*

### A7. Stale ADR references — `CONTRADICTION`
FDR 003:67–68 cites *"the pauperfuse reconciliation model (ADR 009)"*. ADR 009 in this
repo is `009-facet-set-routing-frontier.md`. Reconciliation is ADR 011 (and the vtree
store is ADR 010). Sweep the FDRs for `ADR 008`/`ADR 009` and retarget or delete.
Housekeeping found while checking: **two ADRs numbered 007**
(`007-doc-branch-identity.md`, `007-plug-manifests-as-drawer-docs.md`).

*Owner: editorial pass across FDRs 001–004.*

### A8. "Redaction" is supersession — `WORDING`
FDR 003:149–152: *"**Redaction requires a new doc id.**"* You cannot make an old replica
forget; what you do is publish a successor and abandon the old doc. The mechanism is
right, the word promises erasure. Call it **supersession** (keep the `--redact` verb if
the CLI already has it, but define it as "supersede").

*Owner: FDR 003 §4.*

### A9. Auto-commit on every `.dtree` command vs `db status` revealing un-ingested edits — `CONTRADICTION`
FDR 004:36–39: *"Every `.dtree`-related CLI invocation acts jj-style: it first
auto-commits the working set"*, reinforced at FDR 004:159–161 and :190–192.
FDR 003:64–68 and :85 define `db status` as revealing *"un-ingested local edits"* as the
three-way (last-applied ↔ real tree ↔ branch of record).

If everything auto-commits, the state `db status` exists to show is already published.
The jj analogy is not exact: jj snapshots into the working-copy commit, which nobody
pulls until you push; our "commit" writes the *branch of record* that peers sync.

**Recommendation.** Split observation from mutation in FDR 004 §6:
- **observation** (`db status`, `db diff` without a mutation flag, `db log`): reads the
  three-way state, applies nothing. It may refresh *observations* (scan the fs, update
  the rep) — that is not publication.
- **mutation** (`db commit`, `db adopt --import-now`, `db watch` cycles): performs the
  ingest.
Precedent already exists in FDR 004:190 ("pure reads like `db diff --at`"). `db status`
belongs in that class. **[intent]** msg 16 #1 wanted the always-moving checkout; that
stays true for mutating commands and for watch mode — it does not have to be true of
`status`.

*Owner: FDR 004 §6.*

---

## B. Real holes (need a decision, not an edit)

### B1. A claim alone licenses deleting dirty bytes — `HOLE`
`fs.rs:468–470`: `may_remove` returns `recorded.claim.is_some()`. `bridge.rs:143–152`
calls it for every path the source has dropped. So: daybook made `foo.md` (claim set),
the user then edited it locally, daybook drops the path → the pass deletes the user's
bytes. `Outcome::target_only` protects only paths with *no* claim.

The design already has what the stronger rule needs: the recorded entry carries the stat
fingerprint and (per §B3) content evidence, and `settle`/`accept` already know how to ask
"did this change since we recorded it".

**Recommendation.** State the invariant once, in ADR 011 §8 (deletion):
> A destructive propagation requires the target to be provably **unchanged since the
> correspondence was recorded**: same stat, or (if the stat moved) a digest matching the
> recorded evidence within `hash_limit`. Anything else — a claim with drifted bytes, an
> unreadable record, a record that was lost — means **preserve**.

Then implement it in the backends, with one shared helper (`Entry::unchanged_since`) so
each backend does not reinvent the predicate. This is the review's best structural point
and it costs a helper + a doc line, not an architecture.

*Owner: ADR 011 §8 + `pauperfuse::fs::may_remove`.*

### B2. The recorded half of the rep is not a cache — `HOLE`
ADR 011:175–178: *"**Backend truths, rep caches**: any store corruption or stale rep
heals via a fresh change report. Worst case is re-hashing, never data loss."*

True of **observations** (paths, kinds, sizes — a fresh scan reproduces them). Not true of
**bindings**: `claim` ("we put this path here"), the daybook side's slot→path assignment,
staged-branch pointers and adopt plans (FDR 004 §4 lists exactly these as `.dtree` state).
Losing them:
- cannot authorize deletion — the current fallback is conservative (no claim → no
  removal), which is the right direction, but it is an accident of `may_remove`, not a
  stated invariant;
- *does* lose path identity: a re-materialization re-allocates names, so a real path can
  move under a user (the thing Binding stability exists to prevent);
- loses the source side's "which heads did I last render", which is what makes "who
  changed?" answerable at all (§B3).

**Recommendation.** Two tables, two contracts, named in ADR 010 §3.3 / ADR 011:
- **observations** — rebuildable, droppable, refreshed freely;
- **bindings/correspondence** — durable; loss must degrade to *conservative and
  re-allocating*, never to deleting. Say that aloud, because it is a user-visible
  consequence.
Cheap way to pin it: a test that wipes the observation half and asserts (i) no bytes lost,
(ii) nothing is removed, (iii) with bindings intact, no path moves.

*Owner: ADR 011 §"backend truths", ADR 010 §3.3.*

### B3. Nothing records the source side of a correspondence — `HOLE` (partly planned)
For a checkout, "I materialized doc D at heads H through lens L into path P" is not
recorded anywhere the core can see — deliberately, because the core may not interpret
either side's tokens. **[intent]** the operator: *"daybook backend will maintain its own
state in the `.dtree`. It's pinning which heads it last saw from the node… it can track
lens-specific concerns itself if the others need not to know."*

So the answer is not a core structure; it is a **named requirement** that has not been
written down: every backend that has a "revision" concept must record, per bound path,
the revision it last *produced* or *ingested*. Without it, "both sides changed" is
undecidable and ingest cannot choose a base (see §B4).

**Recommendation.** ADR 011 §(checkout state) states the requirement generically ("a
backend that can name its own revision records the one it last reconciled, per bound
path") and ADR 012 §(render) states the daybook shape (per output slot: doc heads, lens
id/version, dependency fingerprint). The core stays ignorant.

*Owner: ADR 011 + ADR 012.*

### B4. The lens contract has no base state and no dependency set — `HOLE`
ADR 012:79 `async fn ingest(&self, delta: &Delta) -> Result<Vec<DocOp>, Bounce>` and
:82 `async fn render(&self, doc: &DocState, prev: Option<&DocState>) -> Res<Vec<EntryDelta>>`.

`ingest` is not told **what the user's bytes were derived from**, so a file edited
against render R1 while the doc moved to R2 can only be applied as a full-state
overwrite dressed as a CRDT edit. `render` sees one doc and no dependencies, while FDR 001
lets a claim span several facets and lens output span several files.

**[intent]** the operator's own proposal for the cheap fix: *"you could easily do the diff
at a facet level instead by requiring every lens first do import lensing on cur bytes."*

**Recommendation** (informed by the review's shape):
- `render(deps, config)` where the lens first *requests* named dependencies (facets, blob
  refs, a projection binding) through the host, so the dependency set is explicit and a
  slot revision is computable;
- `ingest(base_render, edited_bytes, current_deps, config)` — the base is part of the
  contract, not optional;
- host computes the facet-level diff from `base_render`→`ingest(edited)` and applies it
  against current state, so a lens author writes bytes↔facets, never CRDT surgery;
- an optional `describe_change` for pretty diffs; correctness never depends on it.
This also delivers `db log <path>` as a projection of history through the slot's
dependency set, vs `db log --doc` as the full history — answering the operator's earlier
question about which history a checkout shows.

*Owner: ADR 012 §3/§4. Cross-doc dependency needs an FDR 001 cross-check (a claim that
renders from another doc's content is a dependency, not a copy).*

### B5. `Bounce` must not conflate broken data with a broken runtime — `OPEN`
ADR 012:162 makes the `Bounce` the lens's only conflict surface; ADR 012 OQ1 already asks
for *"error taxonomy of `Bounce` (validation vs transient)"*.

Answer it: **only a semantic validation failure may bounce** (→ FDR 003's branch-on-conflict
policy, `/tmp/conflicts/<facet-id>`). A missing blob, a WASI trap, a codec bug, an
unavailable lens version are *operational* failures: report, retry, or block the checkout
with a status line — never manufacture a conflict branch, and never lose the user's bytes.
Also make the conflict path unique per (doc, facet, incarnation): `/tmp/conflicts/<facet-id>`
collides across docs and across repeated bounces, and a bounce path may hold the only copy
of a failed ingest, so it must not be pruned like scratch state.

*Owner: ADR 012 §5 + FDR 003 §(conflict policy).*

### B6. Aliasing safety rests on the wrong side of the link — `HOLE`
ADR 010:193–199 correctly requires the **source** to be immutable before a hardlink, and
`backend.rs` (`Capabilities::immutable_content`) and `bridge.rs:190–205` implement exactly
that, plus `Accepted::ByReference` must be *asked for* by the target. But immutability of
the source does not make the *aliased pair* immutable: the checkout path shares the inode,
so an in-place write through the checkout (or through a careless app) mutates the blob
store's file.

What saves us today is not stated as a requirement:
- the checkout's own writes are atomic-replace (`write_file_atomic`: scratch + rename) —
  so *we* never write into a shared inode;
- a user edit through an editor that saves by rename + replace breaks the link safely;
- a user edit *in place* (or an app that `chmod`s then writes) corrupts the blob.

**[intent]** the operator's own mitigation: *"it must make them unwritable through perms
or it must be smart to recheck their stat+hash when accessing/serving them again."*

**Recommendation.** Three lines in ADR 010 §2.5 / ADR 013: aliased sources must be
**enforced** read-only (perms) by their owner; receivers write by atomic replace only;
the blob side rechecks stat before serving an externally-tracked location, and a drift
re-ingests as a new version. Residual risk (an app that chmods and writes in place) is
accepted and documented, not defended against with copies.

*Owner: ADR 010 §2.5 + ADR 013 (blob side).*

### B7. A writer mid-file can be observed as a coherent snapshot — `HOLE`
No stability check exists in the fs backend (grep for `unstable`/`before and after` in
`fs.rs`: no matches). The scan stats and hashes; if the file changed between the stat it
recorded and the bytes it read, the torn read *is* what gets recorded, and the next scan
self-corrects only after that record may have been transferred onward.

**Recommendation.** stat → read/hash → stat; if the identity moved, discard the
observation and either retry once or mark the path unstable for this cycle (reporting it
as such, not as a change). Platform hints (`IN_CLOSE_WRITE`, Windows sharing info) are an
optimization on top, never the correctness argument. A recording camera should read as
*unstable*, not as a sequence of new versions.

*Owner: ADR 010 §2.4/§3 + `pauperfuse::fs`.*

### B8. Upstream already invalid — `HOLE`
A local edit can be validated before it reaches main, so bouncing it to a conflict branch
is honest. An upstream change that has already synced into main cannot be relocated: main
is authoritative and immutable. So the model needs a third thing:
> **renderable frontier**: the checkout keeps rendering last-good heads while reporting
> `blocked { current: H7, reason: facet invalid }`, with a local repair affordance.

**[intent]** the operator: *"we must model error cases where upstream has already
corrupted the facet… For facet version bumps, we can provide lenses to support breaking
changes but it's reasonable that bad cases will pop up. The porcelain must plan for
this."*

*Owner: FDR 003 §(status/conflict) + ADR 011 §5. This is the "in-conflict" status line
FDR 004 §9 needs.*

---

## C. Misinformed, or already decided elsewhere

### C1. "You need a third state / a correspondence archive" — `DECIDED` (in part)
The third state exists, in two places:
- `bridge.rs` records the target's rep **from what was written, not what was planned**
  (module docs; the `recorded` vec in `reconcile`), which is what makes "did the target
  change since we touched it" answerable;
- FDR 003:64–68 defines `db status` *as* a three-way: last-applied ↔ real tree ↔ branch
  of record.

What is missing is not the primitive but (i) its durability being admitted (§B2),
(ii) the source-side half being named (§B3), (iii) the deletion predicate (§B1). This is
the single most important thing to get right in the discussion: **a `SyncLink` table would
add a structure where we need a contract.**

### C2. "An N-way bridge needs arbitration: who wins?" — `MISINFORMED`
`bridge.rs` module docs: *"A pass is deliberately one-directional… Running it in both
directions is a mirror."* `reconcile(source, target, store)`. Who wins is answered by the
target that holds the path (`accept`, `may_remove`), with both sides' consent required
for an alias (ADR 010:147–152, :423–429). N backends = a composition of pairwise passes;
there is no implicit merged truth anywhere in the implementation.

The one stale artifact: ADR 010:345's pseudocode `for path in merge(reps, target)` — it
presents a *merged* view of all reps to a target, which is not what `reconcile` does (it
is pairwise, and it also has `Stub` and `Touched` cases the pseudocode omits). Fix the
pseudocode; keep the posture.

### C3. "Projection slots must be visible to the core" — `DECIDED` (no)
Core `origin`/`claim` are opaque, scheme-tagged tokens (`entry.rs`); the core compares and
stores them, never interprets them. Slots, dependency fingerprints, lens versions and
pinned heads are daybook-backend state (operator's answer in the review, and **[intent]**
msg 36–38: "does the core know about lenses? … this is a vocabulary leak"). Path-addressing
stays sufficient for moves, because a rename is add+remove and the digest travels.

If a daybook-side slot identity ever needs to survive *inside* the core, the extension is
one optional opaque per-entry key — not a new core concept — and we do not add it until a
backend needs it.

### C4. "/trash is a reserved namespace and contradicts FDR 001" — `MISINFORMED`
See §A6: it is a default-exclusion clause over an ordinary dpath. The docs' wording is the
bug.

### C5. "Track-only adopt is impossible, so there must be proposed bindings" — `MISINFORMED` (naming)
The review's *conclusion* (there is a plan that is not yet a dpath) is right; its premise
("so the architecture is broken") is not. It is §A5's vocabulary fix. Track-only adopt
creating only `.dtree` is a decision, and a good one (FDR 004 §3 step 4: the expensive path
is always explicit).

### C6. "Import idempotency is false" — `WORDING`
Within one doc, the dpath facet keyed by the dpath is exactly the designed convergence for
one-doc/many-paths representations (code directories — **[intent]** msg 2 #6). Across
devices, two independent imports mint two docs and collide, so the claim must be scoped:
> Re-running an import against the **same binding** is idempotent. Independent imports of
> identical bytes on two devices are two logical objects that will collide at the dpath;
> that is the collision path, not idempotency.

The identity must come from the binding (the checkout records the target doc), not from
the digest: byte-identical files may deliberately be distinct objects.

### C7. "Redaction is really supersession" — `WORDING` → §A8.

### C8. "The hardlink rule is unsafe as written" — `HOLE` (half of it)
The review inferred we hardlink into mutable checkouts without consent. We do not: the
target must ask (`Accepted::ByReference`) and the source must advertise immutability
(ADR 010:193–199, `Capabilities`, `bridge.rs`). The real gap is §B6: enforcement.

### C9. "FDR 001 renders conflict markers" — `CONTRADICTION` → §A2 (verified, not misinformed).

### C10. Small ones worth a line each
- FDR 002 (nodes) vs FDR 004 (`db init [name]`, node display names): nodes are not
  user-named in the model but are named in the porcelain — say "display name is local
  annotation, not identity" once.
- "Relays are just long-lived nodes" vs a multi-tenant relay account model: reconcile the
  wording (a relay is a *node run by someone else*, not a node in your agent's identity
  set).
- Trash with several dpaths, and restoring trash, need the previous claims to be
  remembered — that is a binding/path-history requirement, not a trash one (§B2's durable
  half).
- "No daemons yet" (**[intent]** msg 25) is still the right call: `db watch` is a CLI
  command, not a service.

---

## D. Interop scenarios worth adding as test cases

The review's six are good; each one bites a specific part of our design:

1. **Editor atomic save** (temp + rename): path stable, inode replaced. We already write
   this way (`write_file_atomic`); test that a rename-observed change is a *content*
   change, not a removal+add that trips deletion logic.
2. **Long-running in-place writer** (video recording, download, log): → §B7's unstable
   observation. Must not produce a version per scan.
3. **Multi-file transactional app** (SQLite + `-wal`): pins the non-promise — the fs
   backend guarantees *byte stability per file*, not application consistency; multi-file
   consistency is a specialized backend/lens concern (`sqlite3_backup`-style snapshot).
   Worth stating in ADR 010 §2 as a deliberate limitation.
4. **Application bookkeeping junk** (`.git/`, `node_modules/`, `.obsidian/`, lock/temp
   files): selection/exclusion must be a checkout query concern, never a core heuristic.
5. **Case-insensitive / case-normalizing filesystem** (`foo` vs `Foo`): two claims that
   are distinct dpaths but one real path. Surface it; never overwrite. This is a
   materialization-layer collision our Rule 1/2 algebra does not currently mention.
6. **Cloud placeholder files** (iCloud/OneDrive: entry exists, bytes not hydrated): the
   real-world `Avail::Stub`. Materializing one must not force a download, and reads must
   report unavailability.
7. **(ours) Checkout on a filesystem without hardlinks** (FAT/exFAT, some Android
   providers): `ByReference` must degrade to `Bytes` without an error.
8. **(ours) Hostile path names**: Windows reserved names (`CON`, `NUL`), trailing dots and
   spaces, `<>:"|?*`, long-path limits. The `.d` suffix machinery interacts with all of
   these, and Android's content API has its own rules.
9. **(ours) Aliased blob edited in place** by the user (§B6): assert the drift is detected
   and re-ingested as a new version rather than silently corrupting the serving side.
10. **(ours) The repo's own shape**: a directory that is a git worktree (`.git` *file* in a
    worktree, `.git/` *directory* in a clone) is a file-vs-dir collision at one path — a
    natural Rule 2 case, and a good check that we never touch it.

---

## E. The "correspondence archive" proposal — assessment

The review's structural claim: the vtree is buckling because it is simultaneously a
backend snapshot, a sync base, a provenance tracker, a path-binding database and a
transfer planner; split **observation** from **correspondence**, keep a durable
last-successful-correspondence archive per sync edge, make edges pairwise, and require
"target unchanged since correspondence" for destruction.

**Where it is right.** The *distinction* is right and we should adopt its consequences:
- reps are observations (droppable) — already true;
- correspondence is durable (not droppable) — true, unstated (§B2);
- destruction requires target-unchanged — right, and we currently use a weaker predicate
  (§B1);
- metadata loss must be conservative, never destructive — right, and today it is only
  accidentally so (§B1/B2);
- content strategy (copy/reflink/hardlink/re-render) must not affect who wins — already
  true: `accept` decides and `transfer` only carries out the answer (ADR 010 §2.5).

**Where it is the wrong shape for us.** A shared `SyncLink { edge, left_entry,
left_revision, right_entry, right_revision }` table:
- puts cross-backend relations and *revisions* in the core, which by ADR 010 §2.3 cannot
  interpret either side's tokens — exactly the vocabulary leak we keep peeling back;
- is per-*pair* state, so it needs a home for pairs of backends that may never coexist in
  one store; per-rep halves keep it next to the rows they describe and inside the same
  transaction as the rep apply (the one-txn property `store.apply` already relies on);
- would be a fourth copy of facts we already hold: the fs half is `claim` + recorded stat
  + content evidence; the daybook half is slot → heads/lens/deps.

**The decomposition that gets the whole benefit.** Each side keeps its own half of the
correspondence, and "both changed" is computed from the halves — which is Unison's
archive, split along an ownership line instead of a table line:

| question | who answers | from what |
|---|---|---|
| did the target change since we last touched it? | the target backend | recorded stat, then digest vs recorded evidence (`settle`/`accept` logic) |
| did the source change since we last rendered/synced it? | the source backend | its own last-reconciled revision per bound path (§B3) |
| which path does this slot own here? | the backend that binds | its binding table (durable, §B2) |
| may I destroy this? | the target | §B1's predicate |

**Concrete consequences if we adopt it:** `origin` keeps carrying content identity only
(the review is right that stuffing provenance into it invites ping-pong); `claim` stays
exactly as it is — "we put this path here" — but is documented as *half a
correspondence*, which is what makes §B1 and §B2 obvious; `rep-before-cycle` stops
pretending to be a merge base, because "the base" is *the recorded revision on each side*,
not "the rep as of a moment ago"; `db status` can scan freely without publishing.

**What I would not do.** Do not add a core `SyncLink`, a core `ActivityId`, or core
projection slots. All three are expressible as per-backend durable facts plus one
statement in the ADRs, and each would drag a foreign vocabulary into a crate whose whole
value is being ignorant of it.

**Suggested wording for the invariants** (to land in ADR 010 §3.3/§4 and ADR 011 §8):

1. Backends own truth; a rep is an observation. Refreshing a rep never means accepting or
   propagating a change.
2. Reconciling is pairwise and directional. There is no implicit N-way merged truth.
3. Each side of a pair records the revision it last reconciled, per bound path. That pair
   of records *is* the correspondence; losing either degrades to "cannot prove", which
   means preserve.
4. Destruction requires proof that the target is unchanged since that record.
5. Metadata loss is conservative and re-allocating: never destructive, sometimes
   path-moving. Say so out loud.
6. Content-transfer strategy never affects who wins.
7. Observation commands may refresh observations; only mutations publish.
8. Canonical history stays backend-native; names, activities and file histories are
   annotations over it.

---

## F. What to lock, and in what order

**Blocking (before more CLI code):**
1. §A1 the collision algebra (one principle; kill the file-vs-file dir spill).
2. §B1 the deletion predicate + §B2 the observation/binding split (they are one decision).
3. §B3/§B4 the lens contract: base state, dependency set, per-slot revision.
4. §A9 observation vs mutation (it decides what half the CLI does).
5. §A2 conflict markers (one paragraph in FDR 001, delete the stale one).
6. §A4 checkpoints as a frontier map + the cross-doc correlation decision.

**Right after:**
§A5, §A6, §A7 (wording sweeps); §B5 (`Bounce` taxonomy); §B6/B7 (aliasing enforcement,
unstable observation); §B8 (renderable frontier); §D test cases.

**Do not do:** a core `SyncLink` table, core projection slots, N-way merge semantics,
hardlink cleverness as a correctness mechanism.
