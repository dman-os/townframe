# FDR 003: History, branches, and changes

**Status:** Draft for review. This document defines user-visible version-control operations for Daybook documents and checkouts. It does not require a checkout-wide history object or a single transaction across documents.

## Purpose

Daybook keeps document histories and permits concurrent work without a Git-style staging area. People need to see what changed, inspect older versions, experiment without changing ordinary work, and recover a local edit that cannot be accepted. A checkout can contain files from several documents; a document can produce several files. Its filesystem tree is therefore not itself one versioned document.

The CLI and GUI should present the same underlying concepts: a logical document identified by its main document, its versions, its branch relationships, a checkout's selection of versions or branches, and differences between what was rendered and what is on disk. A branch has its own underlying document ID and history, but the ordinary UI shows it *under its logical document*, not as another unrelated top-level document. Drawer admission and authority for the underlying branch still require explicit treatment; the relationship alone does not grant either.

## Everyday edits

Ordinary edits go to the document of record when they can be represented. There is no mandatory `add`/`stage` step before saving. A filesystem change remains local until ingested; watch mode can ingest it as it arrives, and an explicit write command can do so immediately. Successful local ingest persists changes locally; remote delivery depends on sync. It is not a Git commit that can later be rebased away.

`db status`, `db diff`, and `db log` observe without ingesting or publishing semantic edits. They may scan the filesystem and refresh local observations so their answers are current. `db status` distinguishes un-ingested local changes, document changes not yet rendered here, missing content, and edits that the document layer cannot accept. Reading status must not destroy the un-ingested state it reports. A GUI should distinguish the same states without making users learn staging vocabulary.

The CLI may identify a document by its full ID or by a checkout file path whose recorded binding identifies its owning document. One document may own several output files. When that ownership is ambiguous, a CLI operation that changes history must not guess merely from a matching filename or digest. Short, keyboard-friendly contextual IDs may be displayed and accepted where they resolve uniquely; they are abbreviations, not durable identities. A script or saved reference uses the full ID. The exact short-ID alphabet and disambiguation rules belong in the CLI design.

## Logical documents, branches, and checkout selection

A **logical document** is what the ordinary GUI and CLI present in a document list. Its main branch's document ID identifies it. Its branches form a navigable family: each branch has its own underlying document ID, history, access, and possibly different drawer membership, but ordinarily appears as a version-control choice *within that logical document*, not as an independent top-level item. A branch records a source relationship and, where known, the heads from which it was created. Branch names are optional, mutable labels, not IDs. Users can navigate parents and siblings that their node can discover and access. Some branches may have no shared listing or may be local to one node; the interface must not imply it knows every branch everywhere.

A checkout may select a branch for each logical document it displays. That **checkout-local selection** makes it possible to experiment in a separate checkout without changing what another checkout renders. Its paths and bindings still retain the selected branch's underlying ID, while user-facing paths and history navigation refer back to the logical document. Selecting branch B does not move edits already made to A. Choosing a branch of one logical document does not automatically switch unrelated documents in the checkout.

For now, a CLI branch-creation command requires an explicit logical-document ID or file path. `db branch <name>` with no document selection is refused rather than silently forking every document in a possibly large checkout. An explicit selection can name more than one logical document, but success and failure are reported for each; no checkout-wide branch ID or all-or-nothing replicated operation is implied. The CLI should preview scope when an operation creates many underlying branch documents. Names shared across logical documents can help orient users, but names alone do not identify a checkout-wide branch selection. The exact verbs and flags for creation, switching, naming, merging, and removal belong with the CLI FDR.

A temporary branch document can preserve agent work, staging, or a failed local semantic edit without immediately publishing it. It may remain node-local and not replicate. Being temporary does **not** license deletion while it contains the only saved copy of a person's work. The CLI should expose where pending work lives, permit recovery, and require a safe retention policy. There is no promise that every ordinary edit first creates a temporary branch.

## Historical versions and immutable history

One branch's historical version is specified by that branch's heads, not by a global checkout version. Automerge *can* accept a write based on historical heads of the same document, creating changes concurrent with later ones; this is not technically forbidden. The ordinary historical view is read-only as a **product choice**: when a user wants to continue independently from an older state, the interface creates a related branch so newer main changes are not immediately merged into that work. The user may later merge it deliberately. This does not roll back or rewrite main, and changes already published there cannot be relocated retroactively.

A checkout spanning several documents has no single Automerge head. A historical view must say which document version it chose for each displayed document. A user-facing time or label may help select versions, but time is not proof that those versions formed an atomic snapshot. A CLI spelling such as `checkout --at <version>` must not pretend one version argument identifies the entire tree unless its scope explicitly resolves to one document or to a recorded selection. The CLI and GUI may offer historical navigation once that selection behavior is specified.

Creating a successor document and redirecting paths or drawer listings can **supersede** an old document; it cannot promise to erase history already held by other nodes. An actual erasure/redaction policy requires a separate design. Physical garbage collection of local unneeded bytes is distinct from a visible edit, removing a path, or changing drawer membership.

## Differences and history presentation

A checkout can compare its last rendered state with the current filesystem tree and with newer document versions. Users should be able to ask for a path's owning document, the render base, changes on disk, and document changes not yet reflected in that path. A file move may be inferred from observations, but snapshots cannot always prove one: where identical bytes make two stories indistinguishable, existing path bindings win as specified in FDR 001. A diff must not report an inferred move as certain when it could equally be removal plus addition.

For document content, text lenses can show useful textual changes; blob content can initially show that a reference changed or bytes are unavailable without pretending to offer a textual patch. A multi-file lens may explain which output changed while associating that edit with one owning document. CLI text, summaries, and GUI inline rendering may present the same facts differently. The exact diff object shape and Automerge operation projection are technical design, not a requirement that every renderer display raw CRDT internals.

A document's history lists its own changes and any user-facing annotations. A drawer or checkout can show a **derived timeline** of changes across the documents it selected, grouping nearby events for readability. Such a row is presentation, not an authored atomic multi-document commit. Sync can deliver related changes at different times; the view must not conceal partial availability or imply a total order where none exists. Named annotations or messages are optional. A message supplied while ingesting several documents cannot attach to one imaginary Automerge change across them; its precise durable representation needs a separate design.

## Semantic failures and concurrent work

Automerge can incorporate concurrent changes, but an application may be unable to validate or render the resulting facet state. This is distinct from a path collision, a missing blob, or a lens runtime error. The document layer evaluates accepted/renderable content; generic filesystem reconciliation does not define a Daybook conflict type or write conflict-marker text into files.

A local edit is interpreted against the document heads from which its output was last rendered. It can therefore become concurrent with an incoming edit instead of silently overwriting it with the latest value. If the resulting semantic edit is valid, it can land on the document of record. If semantic validation fails, preserve the user's edit on an identifiable local branch and keep the checkout's usable file safe. A failed codec invocation or unavailable data is not itself a reason to create a conflict branch. The exact validation, bounce, accepted-view, and repair rules—including writes when the current document was already invalid—belong in a document-layer ADR.

Remote changes that have already entered a document cannot be transferred out of its history by the checkout. If their accepted state cannot be rendered, consumers need a reported state and a last good render where available; recovery is not described as retroactive bouncing. A merge of a branch can be refused when it would make the accepted result invalid. The user needs a usable repair or rendered-choice experience, not a promise that every CRDT-convergent value is meaningful.

## Removal, trash, and retention

Deleting a file in a checkout is a lens/path edit. It **does not** automatically delete its document or create a `/trash` claim. Deleting one output of a multi-file lens does not automatically delete the other outputs. FDR 001 specifies the filesystem operation contract and how ambiguous observations are handled. Removing a dpath claim changes that address, not document identity or other claims. Removing a document from one drawer changes its official listing there, not its other drawer memberships or its historical bytes.

Trash is a separate **document-deletion product action**, such as a user pressing Delete on a document in the GUI or invoking an equivalent explicit command. `/trash` remains an ordinary dpath convention that some checkouts omit from their selection. A document with several claims or drawer memberships needs an explicit product policy for what the delete action hides, retains, and can restore. Neither placing one claim under `/trash` nor removing a drawer listing proves that all other copies or access paths have vanished. Those details belong in the deletion/CLI design; checkout-file deletion never triggers the action implicitly.

Discarding a branch removes its discoverable relationship or local selection, subject to retention of unsaved work; it does not retroactively erase changes held elsewhere. Reclaiming storage is a later local policy constrained by other references, blob retention reasons, and recoverable work. Temporary branches that preserve bounced edits must not be pruned solely because their name begins with `/tmp` or a timer expired.

## Multi-document limits

Daybook can perform useful operations over an explicit list of documents, but it must not require a collection-level frontier or checkout-wide commit to make ordinary edits work. An Automerge transaction is per document. Multiple document writes may be prepared or coordinated locally, but this FDR does **not** promise an atomic filesystem-and-document transaction across them. Some documents may succeed and others fail; the CLI reports which and supports safe retry or repair. Independently synchronized nodes may also observe those documents at different times even if local storage later gains stronger transactional behavior.

The default filesystem import boundary is **one document per file**. A user can explicitly choose to keep multiple related files inside one document, so they share one history boundary and can be rendered as several outputs. A directory name alone does not imply one document; a shared branch name across several documents does not create one history boundary either. This choice needs clear preview in import/checkout tooling, especially for source trees and large existing directories.

## Decisions deferred to other documents

- The CLI FDR specifies branch command names, explicit selection and switching, short-ID display and resolution, historical checkout navigation, status output, and the document-delete/restore flow.
- The document and lens ADRs define branch ancestry, schema validation, accepted/renderable states, safe bounce and repair, multi-file lens ingest, and message or history annotations. In particular, they must address writes against already-invalid current heads.
- The checkout ADR specifies durable bindings, filesystem observation, removal safety, and local partial-operation recovery without imposing Daybook identities on the generic tree.
- Blob/retention work specifies what can be recovered when a removed checkout file held the only bytes and how GC accounts for every other retention reason.
