# ADR 036: intxt core

**Changed 2026-11-12:** recorded worktrees have no file documents (ledger D59, ADR 040 §4); the file document of §5 is now only a collaborative worktree's.

**Status:** Proposed.

**Depends on:** ADR 019 (drawers), ADR 020 (documents and datum writes), ADR 021 (datum schemas and codecs), ADR 025 (predicates and polling), ADR 031 (manifest-declared collections), ADR 035 (references and URLs). **Vocabularies built on it:** ADR 040 (work capture: repos, worktrees, git, jj, agent sessions), ADR 041 (annotations: comments, threads, structure, seen state). Functional requirements: FDR 006 (intxt), FDR 007 (the review loop). Decisions: `docs/scratch/intxt-ledger.md` (D-numbers below).

Datum tags use the placeholder authority `org.example` until the naming lane settles NSIDs.

## 1. What an intxt is

An intxt is a drawer (D1) that collects the records of some piece of work so people and agents can read them, point at them and talk about them. It has no vocabulary of its own beyond what this ADR defines. Code, commits, agent sessions, comments, CI results and anything a plug adds later are all datums in the drawer's documents, or in other drawers' documents the intxt references (D15, D21). The review loop of FDR 007 is one use of ADR 040's and ADR 041's vocabularies, and has no more standing in the core than a vocabulary a third-party plug adds (D28).

Every vocabulary inside an intxt follows four rules:

1. **Records of history** (D8). A datum records something that happened or was said. Collaborative editing, where a vocabulary wants it, happens on its own documents or branches (ADR 007) and enters the record when something pins it.
2. **Multiplayer** (D13). Anything several writers produce is keyed so that each writer writes its own slot (a UUIDv7, or a key derived from content such as a commit sha, which makes concurrent writes identical). A slot several writers share holds a value whose last-writer-wins outcome is acceptable.
3. **Legible, and hand-writable where people and agents author** (D23, D34). Every datum is readable as plain JSON. Datums that people and agents write (comments, replies, structure, entries) can be written by hand: no field requires a computation the agent can't do; it may need to read a value to copy it, never to derive one. Datums only a recorder writes (worktree ops, file contents, commits) need only be legible. Helper commands for common writes may come later; raw JSON is the default. **Updates are read-modify-write**: a datum write replaces the whole value, and a field missing from it is removed (`docs/wiki/docs.md`, Evolvability), so a writer updates a datum by editing the value it read, keeping fields it doesn't know, never by writing it again from scratch.
4. **Authorship is attribution, not a field.** No datum carries an author field. Who wrote a datum's current version is what the cabinet attests: today the datum signature (ADR 020 §5–§6), possibly node-attested per-change attribution later (ledger F14). Reads return it as `authors` (the actor UUID and its `dtxt:actor:` URL; every author when concurrent writes are current), whatever evidence backs it, so nothing here depends on per-datum signatures existing. People shown as authors are actors resolved to identities by a later identity design; content that claims an author of its own (a git commit's author line, an imported GitHub comment's login) carries that as data and is shown as "via" whoever wrote the datum.

And every reader is robust to gaps: an intxt spans many documents, some in other drawers, and no reader is guaranteed to hold all of them, or all of their history. A missing document, an unreadable part or a pin naming heads not yet synced is rendered as unresolved, never as an error, as a browser renders a page with a missing image.

## 2. Descriptor, entries and body

The intxt's descriptor follows the drawer's own design (ADR 019): one datum identifies the intxt, and sibling datums on the same document each describe one part of it. There is no generic "layer" datum. Each vocabulary describes its own parts with its own **entry datums**, and the generic **`body`** datum lists them in order, as daybook documents already order their facets.

```jsonc
// The drawer's descriptor doc (the DrawerId)
{
  "org.example.daybook.drawer/main": { "layout": { "$type": "org.example.daybook.drawer.singleGroup", … } },

  "org.example.intxt/main": { "empty": true },                   // this drawer is an intxt
  "org.example.intxt.title/main": { "title": "Datum references move to dtxt:", "description": "markdown, optional" },

  // entry datums: one per part, each owned by its vocabulary (ADR 040, ADR 041)
  "org.example.intxt.work.worktreeEntry/0193a…":   { "worktree": "dtxt:datum:zWt/org.example.intxt.work.worktree/main" },
  "org.example.intxt.work.repoEntry/0193b…":       { "repo": "dtxt:datum:zRepo/org.example.intxt.work.gitRepo/0193b…" },
  "org.example.intxt.work.sessionEntry/eb26ecaa…": { "session": "dtxt:datum:zSess/org.example.intxt.work.agentSession/main" },
  "org.example.intxt.annotationsEntry/main":       { "empty": true },  // annotations are found by tag across the drawer (ADR 041 §6)

  // the ordered index of the entries
  "org.example.intxt.body/main": { "order": [
    "dtxt:datum:self/org.example.intxt.work.sessionEntry/eb26ecaa…",
    "dtxt:datum:self/org.example.intxt.work.worktreeEntry/0193a…",
    "dtxt:datum:self/org.example.intxt.work.repoEntry/0193b…",
    "dtxt:datum:self/org.example.intxt.annotationsEntry/main"
  ] }
}
```

- **`intxt/main`** marks the drawer as an intxt. It says nothing else today (`empty: true`, the empty-member convention). Title and description are the generic `title` datum (§5).
- **An entry datum** names one part of the intxt (a worktree's capture, a repo, an agent session, the discussion) and points at where that part starts with plain datum references. Its tag is the vocabulary's own, so a reader that understands a vocabulary finds its parts by tag, and its schema says what it references. How the vocabulary splits its part into documents, or into other drawers, is its own design; the core only fixes the boundary rules below. Entries are slots keyed by instance, so writers adding different parts never conflict, and two writers adding the same part write equivalent values to one slot.
- **`body`** (daybook's `Body { order }`, generic) lists the entries in the order readers show them: tab order, section order. It makes discovery one read instead of a tag scan, and it is where a part's position is chosen. Its `order` is an Automerge list (`x-automerge: list`, ADR 021), so concurrent appends by two writers both land; two writers adding the same entry concurrently leave it listed twice, and readers show it once. An entry missing from `body` still exists and is shown after the ordered ones; a `body` reference to a missing entry is skipped.

**Boundary rules** between vocabularies:

1. **References are plain declared datum references** (ADR 035), never dedicated link datums. Declared references are indexed (ADR 035 §2.6), so the graph between parts, including "what else must a reader load to resolve this part", is a query and cannot drift from the data.
2. **A vocabulary writes only its own documents.** A link lives on the side that knows about it, normally the later or derived one (a comment points at code; a worktree op points at the tool call that caused it); the reverse direction comes from the reference index.
3. **Unknown entries are carried, not operated** (lexicon-authoring). A reader lists them, follows their references, and renders what it finds generically (§4).
4. **Parts may live elsewhere.** An entry may point into another drawer (a private session, L3); readers without access see the entry and an unresolved reference.

Cross-part metadata that cannot be derived from references (for example, that a session's review should be shown next to a given worktree) lands beside `order` in `body` or in the entry datums when a vocabulary needs it.

## 3. Documents

Every document of every part in the drawer is a roster entry (ADR 019), so it syncs with the drawer and inherits its access. Documents are split by what loads together and what is written together: a reader opens only the parts it shows, and a part that grows without bound (a long session, a worktree with many files) splits by document, not by datum count. A cabinet document refuses updates to existing datums at about 500 datums until the node lane's alias-scan fix lands (ledger F10), so a vocabulary keeps documents below that or shards.

## 4. Rendering datums the reader doesn't know

A viewer of an intxt shows datums of vocabularies it was never built for. It finds a renderer per datum tag through a durable collection in the shell (ADR 031 §10: rows contributed by plug manifests, lazy endpoints, nothing started until used), not one owned by intxt, since daybook and other surfaces render arbitrary datums too (ledger P4). A row names either a stateless presenter (ADR 027, a portable view built from data) or a micro to embed, and the placements it supports, from a closed list that grows by amendment, because each placement needs layout support in the viewer:

- inline in a text view, between lines, at a resolved position;
- file- or document-level, above or below a document;
- a thread item;
- a full view.

Stateless presenters are the default. A micro starts only when the reader expands its insert, and a viewer keeps a few recently visible micro mounts alive so scrolling doesn't restart them. Rows give a height hint, and viewers remember the last measured height per datum, so virtualized lists (claytxt's diff view) place inserts without jumps. A datum with no renderer is shown by a generic view derived from its schema (ADR 021). Which row wins when several match a tag is open (§8).

## 5. Generic datums

An intxt uses generic datums wherever one exists, so that micros built for other plugs work on intxt content and the other way round. These come from daybook's existing vocabulary (`daybook_types::doc`: `titleGeneric`, `pathGeneric`, `note {mime, content}`), restated as Lexicon objects.

**Migrate before beta** (D39). No shared design for generic datums exists yet, so intxt owns them under its own authority, `org.example.intxt.*`: a cartridge registers only NSIDs under its authority, and the node refuses a record another holder already registered. Before the beta they move to a shared vocabulary, informed by the atproto community's generic lexicons (`docs/scratch/atproto-generic-lexicons.md`), and stored datums are migrated to the new tags. The same holds for ADR 041's annotation datums.

```jsonc
// org.example.intxt.title/main                 (was titleGeneric, a bare string)
{ "title": "…", "description": "markdown, optional" }

// org.example.intxt.path/main                  (was pathGeneric)
{ "path": "src/big_cabinet_core/reference.rs" }

// org.example.intxt.note/main                  (was daybook.note)
{ "mime": "text/x-rust", "content": "…" }   // content is Automerge Text when a vocabulary edits it in place
```

A document holding a `path` and a `note` is a file; a document holding a `title` and a `note` is a note. A collaborative worktree's file documents (ADR 040 §4) are exactly that plus one provenance datum, so any note viewer or editor opens them. Comments are ADR 041's, which follows the W3C Web Annotation model.

## 6. What needs a nodule

Nothing in the core. Creating an intxt, adding entries and writing datums are plain datum writes through the generic doc methods (D2), from a micro, the CLI, an agent or CI alike. Waiting for change is a `PollDatums` long-poll. A vocabulary may need a nodule for work clients can't do (ADR 040: reading a checkout inside the node).

## 7. Consequences

- The previous descriptor fields `historyDoc`, `notesDoc` and `repo` are gone: entry datums and `body` replace the first two, and repositories are ADR 040 entries, so an intxt can span repos.
- Daybook's generic datums gain Lexicon object shapes in the shared namespace; daybook_core's migration moves its documents to them.
- The vocabularies of ADRs 036, 040 and 041 are drafted as Lexicons in `src/intxt_core`; its tests run every example in these ADRs through declaration, admission and the codec round trip, which is the check that they can be written by hand.
- The intxt micro reads `body` and the entries to decide what to show. Its review UI is one set of renderers over ADR 040 and 041 parts.

## 8. Open questions

1. **Generic datums' real home** (D39): intxt owns them until before the beta, then they migrate to a shared vocabulary. Tags stay under `org.example` until the project's domain is bought (operator).
2. **Renderer precedence** when several rows match a tag: configuration, deferred; Clay's settings system can hold it (operator).
3. **The signer in reads** (ledger L5): `readDatums` returns no signer today; the node lane adds it.
