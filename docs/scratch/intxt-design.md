# intxt: data model v0

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: What an intxt is, the reference (anchor) model, and the datums the alpha's write loop needs: see the diff, see the agent's annotations, reply, annotate, resolve, give a verdict. It is the intxt lane's answer to the node lane's stand-in datums (`townframe-p3N-2:docs/scratch/big-ndk-intxt-sketch.md`) and the diff view's anchor requirements (claytxt lane). Decisions and open items live in `intxt-ledger.md`.

## 1. The product path this has to survive

intxt starts as a review tool, becomes the place PRs happen, and may become a VCS. The model is cut so that each stage adds kinds and doesn't change shapes:

| Stage | What exists | What the model adds |
|---|---|---|
| Review | A change that lives in git (a PR, a branch, a commit range), imported into an intxt | Revisions pointing at git commits; annotations, evidence, sessions referencing them |
| PR replacement | Agents push revisions and wait for verdicts; intxt is where the discussion lives; GitHub sync in and out | Verdicts, intxt state (open/merged/closed), a GitHub link on the intxt |
| VCS | Source lives in Daybook docs, not git | New *target kinds* in references (a native snapshot or changeset). Annotations, evidence and sessions don't change |

The thing that must not move is the reference model (§3). Everything else is cheap to change while there are no users.

A VCS stays separate from an intxt (decided in the design discussion): an intxt **composes** artifacts and doesn't own their histories. A future native repository is its own set of docs; an intxt references it the way it references git today.

## 2. An intxt is a drawer

One intxt is one drawer (operator, 2026-11-10). The drawer is the unit of sharing, sync and retention: mirroring it to a relay, making it public (app-public, see the node requirements' R15), and syncing it to the phone all happen per drawer.

Inside the drawer, docs are split by **who writes them and who needs them**, not by type:

| Doc | Datums | Written by | Why its own doc |
|---|---|---|---|
| **root** (the drawer's descriptor doc) | `org.intxt.intxt/main`, `org.intxt.revision/<id>`, `org.intxt.threadState/<annotationId>`, `org.intxt.verdict/<id>` | anyone with write on the intxt | The small, always-synced spine: what the change is and where the discussion stands |
| **layer** (one per source, many per intxt) | `org.intxt.layer/main`, `org.intxt.annotation/<id>`, `org.intxt.evidence/<id>`, `org.intxt.session/<id>` | usually one writer: an agent run, a CI job, a human reviewer | A layer is what the UI toggles ("agent's guide", "CI", "my comments"). It is also the unit a phone can skip syncing and the unit that can be withheld from a public share |
| **blobs** (drawer-native, ADR 026) | file contents, transcripts, logs | the layer's writer | Bytes don't belong in Automerge |

Layers are the "subtext": a set of statements one source makes about the change. The operator's examples map directly: an agent's guided review is a layer; a CI run's results are a layer; the agent session that produced the code is a layer; the human's review is a layer.

**Why not one doc per annotation:** one Automerge doc per comment multiplies sync and Keyhive cost for no access benefit. **Why not one doc for everything:** a large agent transcript layer would ride along on every phone sync, and could not be withheld from a public share.

**Private sessions.** A drawer's docs share the drawer's access (ADR 019 groups). A transcript the author doesn't want public therefore can't sit in a drawer that becomes public. Proposal: a private layer lives in a **child drawer** of the intxt (own groups, not shared when the intxt is made public), and the intxt references it. Open as L3 in the ledger.

## 3. References

Every pointer from an intxt to something it talks about is a `Ref`. The design rule (from the design discussion): **exact historical identity is the foundation; tracking is a capability on top.** A stored ref never gets silently rewritten to a guess.

```text
Ref      = { target: Target, select?: Selector, quote?: Quote }
Target   = open union, says WHAT and AT WHICH VERSION
Selector = open union, says WHICH PART of the target
Quote    = { exact, prefix, suffix }   the selected text plus a little context, for re-attaching
```

`target` carries the version, so a ref is always pinned. A ref without `select` means the whole target (file-level, commit-level, datum-level).

### Targets (alpha set)

| Kind | Fields | For |
|---|---|---|
| `gitCommit` | `commit` (sha), `repo?` | A whole commit or a file at a commit |
| `gitChange` | `base`, `head` (shas), `repo?` | A comparison: what a diff hunk, diff range or whole-change annotation is *about*. Making the change, not the resulting text, addressable is what lets a comment target a deletion or a move |
| `datum` | `datumRef` (`db+datum:` URL), `refHeads?` | Anything in Daybook: another annotation (replies), a revision, a session datum, later native source. Follows the existing `datumRef` + `refHeads` convention (ADR 021 §2) |
| `blob` | `digest`, `mime?` | Bytes that aren't a datum: a log, a transcript file |

`repo` is a hint for fetching (a remote URL), never identity: commit shas are already content-addressed. `Unknown` members are carried, not operated (lexicon-authoring rules), so a later `nativeSnapshot`/`nativeChange` target lands without touching anything else.

### Selectors (alpha set)

| Kind | Fields | On targets | Notes |
|---|---|---|---|
| `file` | `path`, `oldPath?` | gitCommit, gitChange | File-level anchor; the diff view renders it as a file header comment (claytxt requirement c) |
| `lineRange` | `path`, `oldPath?`, `side` (`base`\|`head`), `start`, `end` (1-based, inclusive lines) | gitCommit, gitChange, blob | On a `gitChange` it may span hunks and include unchanged context (claytxt requirement b). A range that touches both sides is two refs |
| `textCursor` | `objPath`, `start`, `end` (Automerge cursors, serialized) | datum | Native tracking inside Automerge Text: Automerge resolves a cursor made at old heads to a position at new heads, so no matching is needed while history is shared |
| `entry` | `id` | blob (a JSONL transcript), datum | One message or tool call in an agent session, by the harness's own entry id |
| `pointer` | `path` (JSON Pointer) | datum | A field of a datum |

Not in the alpha: symbol selectors (structural, language-aware) and a character-precise range inside a line. Both are additive.

### Re-attaching across revisions

When a new revision arrives, a `lineRange` on the old `gitChange` is still exactly right about the old revision; the question is only how to show it on the new one. The client decides at render time, from data it already has:

1. map the range through the diff between old head and new head (exact, when the lines are untouched);
2. otherwise search for `quote.exact` near the mapped position, using `prefix`/`suffix` to break ties;
3. otherwise show it as **outdated**, attached to the revision it was written on.

Nothing is written back. If re-attachment later turns out to need persistence (it is expensive, or users correct it by hand), a separate `relocated` datum records it next to the original, never in place of it. `quote` is required on `lineRange` refs written by tools, and optional for humans' (the micro fills it in).

### The six test cases

| Case | Ref |
|---|---|
| A git commit | `{ target: gitCommit{commit} }` |
| A diff range | `{ target: gitChange{base, head}, select: lineRange{path, side: head, start, end}, quote }` |
| A deleted line | same, with `side: base` |
| An Automerge text cursor | `{ target: datum{datumRef, refHeads}, select: textCursor{objPath, start, end} }` |
| A message in an agent transcript | `{ target: blob{digest}, select: entry{id} }`, or `datum{session datum}` + `entry` |
| A review section spanning files | an annotation with several targets (`targets: [Ref]`, §4), not a composite selector |

## 4. Datums

Shapes are sketches in Rust-ish notation; the real ones follow lexicon-authoring (open objects with `Extra`, open unions with `Unknown`, open string vocabularies). Every write below is a **plain datum write by the client** (operator, 2026-11-10: clients write datums directly; no API over the datums where avoidable). Each concurrent writer writes its own datum keys, so concurrent writes don't conflict.

**Authorship is the datum's signature, not a field.** Every datum write is signed by the writing actor (ADR 020 §4–§6), so "who said this" comes from provenance, and a `author` field would be an unsigned claim that can disagree with it. The read API has to expose the signing actor of a datum's current version; see ledger L5.

### Root doc

```rust
// org.intxt.intxt/main
struct Intxt {
    title: String,
    description: Option<String>,   // markdown
    state: IntxtState,             // open_string: open | merged | closed
    repo: Option<RepoHint>,        // remote URL(s), for fetching only
    external: Vec<ExternalLink>,   // open union; githubPr { owner, repo, number } first. Roadmap: sync
}

// org.intxt.revision/<id>   append-only; a new push is a new revision
struct Revision {
    seq: u32,                      // 1, 2, 3… for display; concurrent pushes may collide, shown as such
    base: GitSha, head: GitSha,
    commits: Vec<GitSha>,          // base..head, oldest first
    files: Vec<FileChange>,        // what the diff view needs without git on the phone
    createdAt: Timestamp,
}
struct FileChange {
    path: String, oldPath: Option<String>,
    status: FileStatus,            // open_string: added | modified | deleted | renamed | copied
    base: Option<BlobRef>,         // full contents at base; None for added
    head: Option<BlobRef>,         // full contents at head; None for deleted
    binary: Option<bool>,
}

// org.intxt.threadState/<annotationId>   one slot per thread root
struct ThreadState { resolved: bool }

// org.intxt.verdict/<id>
struct Verdict { revision: Ref, kind: VerdictKind /* open_string: approve | requestChanges | comment */, body: Option<String> }
```

Full file contents at base and head, not a patch: the phone has no git, the diff view needs context lines beyond the hunk, re-attachment needs the new head's text, and a patch can be recomputed from contents but not the other way round. Blobs are content-addressed, so an unchanged file across revisions is stored once. Very large or binary files can be `None` with `binary: true`; the alpha doesn't render them.

A thread's resolved flag is in the root, not in the annotation's layer, because anyone in the review resolves threads, not only the layer's writer. A `ThreadState` slot is last-writer-wins on purpose: two people resolving at once agree; resolve vs. reopen at once is a visible conflict Automerge already reports.

### Layer docs

```rust
// org.intxt.layer/main
struct Layer {
    kind: LayerKind,               // open_string: guide | review | ci | session | notes
    label: String,                 // "Claude Code — guided review", "CI: nextest"
    producer: Option<Producer>,    // open union: agent { harness, model?, sessionRef? } | ci { system, runUrl? } | human
}

// org.intxt.annotation/<id>
struct Annotation {
    targets: Vec<Ref>,             // ≥1; several for a section spanning files
    motivation: Motivation,        // open_string, W3C-style: commenting | replying | explaining | sectioning | questioning | flagging | suggesting
    body: Body,                    // open union: markdown { text } | suggestion { path, replacement } | …
    revision: Ref,                 // the revision it was written against (datum ref to org.intxt.revision/<id>)
    order: Option<f64>,            // position in a guide; fractional so agents can insert between
    createdAt: Timestamp,
}

// org.intxt.evidence/<id>
struct Evidence {
    subject: Ref,                  // what it ran against: a gitCommit, never "the PR"
    kind: EvidenceKind,            // open_string: test | lint | build | bench | check
    command: Option<String>,
    outcome: Outcome,              // open_string: pass | fail | error | skipped
    summary: Option<String>,       // "412 passed, 1 failed"
    log: Option<BlobRef>,
    startedAt: Option<Timestamp>, finishedAt: Option<Timestamp>,
    environment: Option<Environment>, // open: os, toolchain, runner
}

// org.intxt.session/<id>
struct Session {
    harness: String,               // "claude-code", "codex", …
    sessionId: Option<String>,     // the harness's own id
    transcript: BlobRef,           // the raw log, as the harness wrote it
    format: String,                // "claude-code-jsonl@1", so readers pick a parser
    produced: Vec<Ref>,            // revisions or commits this session produced, as recorded
    provenance: ProvenanceKind,    // open_string: recorded | matched | inferred
}
```

Notes:
- **Replies are annotations** whose target is the parent annotation (`datum` target, `motivation: replying`). A thread is the reply tree under a root annotation. Replies live in the replier's layer, so a human's reply to the agent's note is in the human's review layer.
- **A guided review is a guide layer**: `sectioning` annotations, each with several `lineRange` targets, a markdown body, and an `order`. The UI walks them in order and shows the raw diff underneath. Anything the guide doesn't cover stays visible as "not in any section".
- **Evidence is not an annotation.** It reports that something ran, against an exact commit. An annotation can cite it (`datum` target) to say "this proves X", which keeps "the agent says tests pass" distinguishable from "a run passed against `abc123`". The subject is a commit, so evidence for revision 1 doesn't appear to cover revision 2 unless the head commit is the same.
- **Session provenance** keeps the observed/declared distinction from the design discussion: `recorded` (the harness captured edits as they happened), `matched` (edits matched exactly to commits), `inferred` (fuzzy). The UI must not show them with equal authority.
- **Attestations** (in-toto, SARIF import) can come later as evidence `kind`s and a signed body; out of scope for the alpha.

## 5. Which writes need the nodule

Per the operator, none of the write loop needs it: creating the drawer and docs, adding a revision, annotating, replying, resolving and giving a verdict are plain datum writes through the generic doc methods, from the micro, the CLI, MCP or CI alike. What a nodule (or an agent-side tool) is still for:

| Need | Why a client can't just write | Where |
|---|---|---|
| Building a `Revision` from a checkout | Reading git, computing changed files, uploading contents | Agent-side tool or the intxt nodule with a granted directory (R19, gitoxide on wasip3 to be checked) |
| `awaitResponse` for agents | Agents want "block until the human answers"; it is `PollDatums` over the intxt's docs filtered to annotations not written by the agent | Can be a plain `PollDatums` predicate if predicates can filter by signer; otherwise a thin nodule method |
| Validation the schema can't express | e.g. a reply's parent must be in the same intxt, `Revision.seq` monotonic | Not enforced in the alpha; the UI tolerates violations |
| GitHub sync | Network + credentials | Later; its own nodule |

So the intxt nodule shrinks from six verbs to: **produce a revision from a checkout**, and possibly **await**. Everything else in the stand-in sketch becomes datum writes.

## 6. Not in v0

- Native source (VCS stage), structural/symbol selectors, persisted re-attachment.
- GitHub sync (on the roadmap; not this week).
- Attestation formats and signature verification of CI evidence beyond the datum signature.
- Suggested-change application, merge execution.
