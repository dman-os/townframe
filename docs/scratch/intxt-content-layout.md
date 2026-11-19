# intxt over drawers: content documents, not the drawer document

Status: Accepted 2026-11-18 (operator, ledger D70); written down for the record before ADR 036 and ADR 041 are amended
Owner: intxt lane
Updated: 2026-11-18
Scope: Where an intxt's datums live among its drawer's documents. Supersedes ADR 036 §2's "the drawer's descriptor doc" layout and ADR 041 §6's shared notes document. Related: `intxt-multiplayer.md` (the audit that found this), ADR 019 (drawers, meta docs, relays).

## Why

1. **Relays read the index.** A single-group drawer's own document is its public metadata root: Relay-level members get Read on it (ADR 019, "Meta docs"). intxt writes its whole index there (ADR 036 §2): title, description, entries, their order, the commits document's pointer, and until stack/intxt-26 the pack and diff records. A relay can read every intxt's title and what it holds.
2. **Comments are filed as drawer metadata.** The notes document is a sensitive meta document (`CreateMetaDoc`, root Sensitive). Meta documents describe the drawer; comments are content.
3. **Fixed slots race across nodes.** A meta document's `(root, kind, name)` slot holds one value, and concurrent creators leave last-writer-wins (ADR 019). Two nodes commenting first before syncing each made a notes document, one won the slot, and the other's comments were orphaned (live two-node run, 2026-11-18, diagnosed by nodules). The commits document's single pointer and the blob inventory's slot are made the same way. Get-or-create on the synced roster cannot fix this for writers that are offline.

## The layout

**The drawer's own document** holds only what describes the drawer: the cabinet's descriptor and the intxt marker (`org.example.intxt/main`), plus the pointer to the index document. A relay learns that the drawer is an intxt and the ids of its documents, nothing about the work.

**The index document** is one content document, created by `createIntxt` in the same step as the drawer. Only the intxt's creator ever creates it, so nothing races. It holds the title and description (`title/main`), the entry datums, and the `body` order. `edit` writes here. Concurrent changes to these datums follow the CRDT rules they already have.

**Per-writer content documents** hold everything several people add to. A writer is an attested actor: today, a node's operator. The race exists only between nodes, because command ids already dedupe within one node. So agent sessions on one node share their node's documents, and a comment still names its own `creator` (D69). Refined 2026-11-19, when the work was briefed.
- **Notes:** each participant writes comments, replies, structure and their own seen records into notes documents they created. A reply points at its parent with a concrete reference, so a thread can span documents. A thread's state (`threadState/<root>`) lives in the root comment's document, which exists before anyone resolves it, and anyone may write it there.
- **Commits:** each participant's `add` writes `gitCommit`, `rewrite`/`rewrittenFrom` and the pack and diff blob records into commits documents they created. Commits are content-addressed, so the same commit written by two people is the same datum in two documents, and readers take either.
- **Rotation:** a participant starts a new document of a kind when the current one nears the ~500-datum update limit (F10), as op logs rotate (ADR 040 §7).

**Discovery is by tag, not by pointer.** A reader lists the drawer's content documents and reads each document's kind marker (for example `org.example.intxt.notes/main` with the participant it belongs to) to find every participant's notes and commits. No slot names "the" notes or commits document, so there is nothing to race for. Each participant remembers their own current documents locally.

**The blob inventory** stays a sensitive meta document, since it is the cabinet's own pin-set pattern, but under a name per participant (`blobInventory/intxt-<participant>`), so concurrent first `add`s create two inventories rather than racing for one.

## What a relay can read afterwards

The marker, the index pointer, the drawer's roster (document ids) and its child structure. Not the title, the description, the entries, comments or commits.

## Effects

- **The micro** reads the index document, a member document of the drawer, so the drawer-document read gap that nodules fixed for intxt (node-55) no longer matters to it.
- **The cartridge** (ADR 049 permbinds): the routines write content documents in granted drawers; the descriptor write grant question (the drawer-document write gap) goes away for intxt.
- **No migration:** alpha, test data only.

## Order of work

After stack/intxt-26 (blob records into the commits document), 27 (reply scope), 28 (cause backfill) and 29 (comment creator) land, as one new stack layer: `createIntxt` makes the index document; the readers and writers in intxt_core, day_cli and the micro move to it; notes and commits become per participant with discovery by tag; the inventory name becomes per participant. ADR 036 §2 and ADR 041 §§3, 5, 6 are amended with dated change notes in the same layer.
