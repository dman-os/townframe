# ADR 036: intxt core

**Status:** Proposed.

**Depends on:** ADR 019 (drawers), ADR 020 (documents and datum writes), ADR 021 (datum schemas and codecs), ADR 025 (predicates and polling), ADR 031 (manifest-declared collections), ADR 035 (references and URLs). **Vocabularies built on it:** ADR 040 (work capture: repos, worktrees, git, jj, agent sessions), ADR 041 (annotations: comments, threads, structure, seen state). Functional requirements: FDR 006 (intxt), FDR 007 (the review loop). Decisions: `docs/scratch/intxt-ledger.md` (D-numbers below).

Datum tags use the placeholder authority `org.example` until the naming lane settles NSIDs.

## 1. What an intxt is

An intxt is a drawer (D1) that collects the records of some piece of work so people and agents can read them, point at them and talk about them. It has no vocabulary of its own beyond what this ADR defines. Code, commits, agent sessions, comments, CI results and anything a plug adds later are all datums in the drawer's documents, or in other drawers' documents the intxt references (D15, D21). The review loop of FDR 007 is one use of ADR 040's and ADR 041's vocabularies, and has no more standing in the core than a vocabulary a third-party plug adds (D28).

Every vocabulary inside an intxt follows four rules:

1. **Records of history** (D8). A datum records something that happened or was said. Collaborative editing, where a vocabulary wants it, happens on its own documents or branches (ADR 007) and enters the record when something pins it.
2. **Multiplayer** (D13). Anything several writers produce is keyed so that each writer writes its own slot (a UUIDv7, or a key derived from content such as a commit sha, which makes concurrent writes identical). A slot several writers share holds a value whose last-writer-wins outcome is acceptable.
3. **Hand-writable** (D23). An agent can create or answer any of these datums by writing the JSON itself. No field requires a computation the agent can't do; it may need to read a value to copy it, never to derive one.
4. **Authorship is the signature** (ADR 020 §5–§6). No datum carries an author field; the signing actor of a datum's current version is who wrote it. The read API must expose it (ledger L5).

And every reader is robust to gaps: an intxt spans many documents, some in other drawers, and no reader is guaranteed to hold all of them, or all of their history. A missing document, an unreadable layer or a pin naming heads not yet synced is rendered as unresolved, never as an error, as a browser renders a page with a missing image.

## 2. Descriptor and layers

The intxt's descriptor follows the drawer's own design (ADR 019): one datum identifies the intxt, and sibling datums on the same document each describe one aspect of it, in slots named by kind and name, so independent writers add aspects without touching each other.

```jsonc
// The drawer's descriptor doc (the DrawerId)
{
  "org.example.daybook.drawer/main": { "layout": { "$type": "org.example.daybook.drawer.singleGroup", … } },

  "org.example.intxt/main": { "empty": true },                   // this drawer is an intxt
  "org.example.title/main": { "title": "Datum references move to dtxt:", "description": "markdown, optional" },

  "org.example.intxt.layer/worktree/0193a…": {
    "contains": ["org.example.work.worktree", "org.example.work.worktreeOp", "org.example.work.file"],
    "docs": { "$type": "org.example.intxt.layer#listed", "index": "dtxt:datum:zWt/org.example.work.worktree/main" },
    "dependsOn": ["repo/0193b…"]
  },
  "org.example.intxt.layer/repo/0193b…": {
    "contains": ["org.example.work.gitRepo", "org.example.work.gitCommit", "org.example.work.gitRef"],
    "docs": { "$type": "org.example.intxt.layer#single", "doc": "zRepo" }
  },
  "org.example.intxt.layer/agentSession/eb26ecaa…": {
    "contains": ["org.example.work.agentSession", "org.example.work.promptTurn"],
    "docs": { "$type": "org.example.intxt.layer#single", "doc": "zSess" },
    "dependsOn": ["worktree/0193a…"]
  },
  "org.example.intxt.layer/annotations/main": {
    "contains": ["org.example.comment", "org.example.threadState", "org.example.structure", "org.example.seen"],
    "docs": { "$type": "org.example.intxt.layer#tagged" }
  }
}
```

- **`intxt/main`** marks the drawer as an intxt. It says nothing else today (`empty: true`, the empty-member convention); intxt-wide fields land beside it when one is needed. Title and description are the generic `title` datum (§5), so anything that shows titles shows an intxt's.
- **A layer** (`intxt.layer/<kind>/<name>`) is one body of records in the intxt: one worktree's capture, one repo's git objects, one agent session, the discussion. It states:
  - `contains`: the datum tags a reader finds in it. This is what picks renderers (§4) and tells a reader whether it understands the layer before opening any of its documents.
  - `docs`: how to find its documents, an open union. `single` names one document. `listed` points at a datum that lists them (a worktree's file docs are listed by its worktree datum, ADR 040). `tagged` means "every document in this drawer holding these tags", found with `PollDatums` over `InDrawer` + `HasTag` (ADR 025), for layers with many writers and no owner, such as comments. New ways (sharded, in another drawer) are new members.
  - `dependsOn`: the layers whose datums this layer's datums reference, so a reader knows what else to load to resolve them and a writer knows what it must also include when it adds this layer to an intxt.
- `<kind>` is an open string. It names the kind of layer, which is not a datum tag: a vocabulary may contribute several kinds, and a layer may hold several vocabularies. `<name>` is whatever identifies the instance (a worktree's UUID, a session id, `main`).
- Each layer is its own slot, so two writers adding different layers never conflict, and two writers adding the same layer write the same slot with equivalent values.
- A layer whose documents are in another drawer (a private session, L3) uses a `docs` member naming that drawer. Readers without access to it see the layer and an unresolved reference (ADR 035 §2.1).

Unknown layer kinds and unknown `docs` members are carried, not operated (lexicon-authoring): a reader lists them and renders their datums generically (§4).

## 3. Documents

Every document of every layer in the drawer is a roster entry (ADR 019), so it syncs with the drawer and inherits its access. Documents are split by what loads together and what is written together: a reader opens only the layers it shows, and a layer that grows without bound (a long session, a worktree with many files) splits by document, not by datum count. A cabinet document refuses updates to existing datums at about 500 datums until the node lane's alias-scan fix lands (ledger F10), so a vocabulary keeps documents below that or shards.

## 4. Rendering datums the reader doesn't know

A viewer of an intxt shows datums of vocabularies it was never built for. It finds a renderer per datum tag through a durable collection in the shell (ADR 031 §10: rows contributed by plug manifests, lazy endpoints, nothing started until used), not one owned by intxt, since daybook and other surfaces render arbitrary datums too (ledger P4). A row names either a stateless presenter (ADR 027, a portable view built from data) or a micro to embed, and the placements it supports, from a closed list that grows by amendment, because each placement needs layout support in the viewer:

- inline in a text view, between lines, at a resolved position;
- file- or document-level, above or below a document;
- a thread item;
- a full view.

Stateless presenters are the default. A micro starts only when the reader expands its insert, and a viewer keeps a few recently visible micro mounts alive so scrolling doesn't restart them. Rows give a height hint, and viewers remember the last measured height per datum, so virtualized lists (claytxt's diff view) place inserts without jumps. A datum with no renderer is shown by a generic view derived from its schema (ADR 021). Which row wins when several match a tag is open (§8).

## 5. Generic datums

An intxt uses generic datums wherever one exists, so that micros built for other plugs work on intxt content and the other way round. These come from daybook's existing vocabulary (`daybook_types::doc`: `titleGeneric`, `pathGeneric`, `note {mime, content}`), moved into a shared namespace as Lexicon objects:

```jsonc
// org.example.title/main                 (was titleGeneric, a bare string)
{ "title": "…", "description": "markdown, optional" }

// org.example.path/main                  (was pathGeneric)
{ "path": "src/big_cabinet_core/reference.rs" }

// org.example.note/main                  (was daybook.note)
{ "mime": "text/x-rust", "content": "…" }   // content is Automerge Text when a vocabulary edits it in place
```

A document holding a `path` and a `note` is a file; a document holding a `title` and a `note` is a note. ADR 040's file documents are exactly that plus one provenance datum, so any note viewer or editor opens them. Comments are ADR 041's, which follows the W3C Web Annotation model.

## 6. What needs a nodule

Nothing in the core. Creating an intxt, adding layers and writing datums are plain datum writes through the generic doc methods (D2), from a micro, the CLI, an agent or CI alike. Waiting for change is a `PollDatums` long-poll. A vocabulary may need a nodule for work clients can't do (ADR 040: reading a checkout inside the node).

## 7. Consequences

- The previous descriptor fields `historyDoc`, `notesDoc` and `repo` are gone: layers replace the first two, and repositories are ADR 040 layers, so an intxt can span repos.
- Daybook's generic datums gain Lexicon object shapes in the shared namespace; daybook_core's migration moves its documents to them.
- The intxt micro reads layers to decide what to show. Its review UI is one set of renderers over ADR 040 and 041 layers.

## 8. Open questions

1. **Namespace owner** of `title`, `path`, `note`, `comment` and the other generic datums: big_cabinet_core's own vocabulary, or a shared core-datums plug. Tags stay under `org.example` until the project's domain is bought (operator).
2. **Renderer precedence** when several rows match a tag: configuration, deferred; Clay's settings system can hold it (operator).
3. **Layer `contains` granularity**: datum tags (as above) or vocabulary ids (one per plug or ADR). Tags are what renderers key on; vocabulary ids are shorter and stable as a vocabulary grows.
4. **The signer in reads** (ledger L5): `readDatums` returns no signer today; the node lane adds it.
