# FDR 001: Dpaths and filesystem checkouts

**Status:** Draft for review. This document defines intended user-facing behavior; collision naming and lens selection still need technical specifications.

## Why paths matter

This functional design describes how people address Daybook content with dpaths and work with it in filesystem checkouts. It covers the meaning of claims, what ordinary file operations should do, and the limits of inferring intent from a filesystem snapshot. It does not define storage schemas or the reconciliation algorithm; the ADRs do that.

Daybook documents have stable identities and CRDT histories, but they are not files. A document contains facets; a branch is a separate document with its own identity and history. A document might represent one note, many source files, or metadata for a photograph. People and existing tools instead work with paths: they edit notes in a familiar directory, import an Obsidian vault, browse DCIM, or use a filesystem from a sandbox. Daybook therefore needs both a way to address content with path-like labels and a way to work with it through ordinary files.

These are different concerns. A **dpath** is a claim made in Daybook. A **checkout** selects content, gives it filesystem paths, records what it rendered, and reconciles later filesystem edits. The Daybook side of a checkout can also provide paths by other means: `/by-id` is not a collection of dpath claims. Other path sources can be added without redefining dpaths. Generic filesystem/tree reconciliation does not need to understand documents, facets, or lenses; its Daybook backend does.

!> this FDR is introducing FDRs because you're replacing the FDR replacement ADR! but you're just jumping into the fucking discussion!

The checkout is for interoperation, not a demand that every document look like one file or that every legal dpath has the same literal spelling on every filesystem. It must favor stable, usable paths over aesthetically uniform collision results.

## Dpath claims

A dpath is a UTF-8 path-shaped label beginning with `/`. Segments are separated by single `/` characters, cannot be empty, and cannot be `.` or `..`. The initial `/` is part of the dpath; no additional leading or trailing separator is allowed. Daybook compares the UTF-8 bytes exactly: it does not fold case or normalize Unicode. It imposes no uniqueness constraint. A doc can claim `/inbox/hello.md` and `/projects/daybook/hello.md`; another doc can claim either one. `/a/b` is legal without a claim on `/a`: a checkout may make `/a` an implicit directory.

A dpath is both an address and a useful prefix-filtering label. Selecting `/inbox/**` can provide an inbox; selecting `/tagged/urgent/**` can serve a tagging convention. Such a label is still a path claim, however: selecting it for projection may produce a file or collide with another claim. Other, non-path tag facets remain possible. Daybook reserves no dpath subtree, including `/trash` and `/by-id`. Most checkouts can exclude `/trash/**` by default without making `/trash` a special Daybook data type.

A local filesystem may compare names differently or reject names Daybook accepts. The checkout must account for case-insensitive names, Unicode normalization, and platform-forbidden names without changing the underlying dpath. Whether and how an arbitrary non-UTF-8 filesystem filename can be imported into a UTF-8 dpath is **not decided** here; an import must not silently replace an unrepresentable name with a different identity.

### Facet representation

A dpath is declared by a facet in the **claiming document**. The facet tag is `org.example.daybook.dpath`, and its key ID is the entire dpath, including its leading slash. Thus the key for `/photos/beach.jpg` is `org.example.daybook.dpath//photos/beach.jpg`. The existing facet-reference grammar can address this facet: `db+facet:///<doc-id>/org.example.daybook.dpath//photos/beach.jpg`. Everything after the facet tag is its ID; the double slash is intentional. This FDR does not extend the URL grammar to fields within facets or specify dpath URLs.

A claim with no explicit targets addresses the claiming document's user-visible content. Its Body facet identifies the primary content for lens selection; other user-visible facets may contribute according to that lens. A selective claim lists facets to project. References can specify heads, including the existing empty-heads same-transaction convention; the URL and head rules are defined by the facet-reference design, not invented here. For example:

```jsonc
{
  "org.example.daybook.dpath//inbox/hello.md": {},
  "org.example.daybook.dpath//photos/beach.jpg": {
    "targets": [
      { "facetRef": "db+facet:///self/org.example.daybook.blob/main" },
      { "facetRef": "db+facet:///self/org.example.daybook.imagemetadata/main", "refHeads": [] }
    ]
  },
  "org.example.daybook.dpath//DCIM/other.jpg": {
    "facetRef": "db+facet:///<other-doc-id>/org.example.daybook.blob/main",
    "refHeads": ["<hash>"]
  }
}
```

The last claim lives in a document the author can edit and points at another document, which the author need not control. That makes read-only addressing possible; it does not grant access to the target, ensure the target exists locally, or make its bytes available. The claimant and target identities must remain distinct in status and history. No one editable output may silently combine several document histories: the precise admissible scope of cross-document references and read-only projections needs a lens contract before implementation.

The facet key permits one claim slot per exact dpath **within the same document**. Repeated assignment there converges on that slot; it does not unify two independently created documents that happen to import the same path. Multiple claims on one document are independent facets; removing one does not erase the others. Dpath facets are ordinary document facets and have ordinary history. A branch document does not inherit drawer membership or dpath claims merely because it records an origin; copied or new claims have to be accounted for explicitly.

A dpath claim describes what is addressed, not how to encode it. Lens selection may consider an extension, blob MIME information, and separate customization facets; no lens ID, MIME assertion, or rendering parameters are put into the dpath value solely to make projection work.

## Checkout selection and path sources

A checkout chooses what content to expose and where it will appear. Dpath-prefix queries are one useful selection mechanism, not the only possible source of paths. The `/by-id` surface supplies a full-JSON view of dpathless documents for consumers such as a WASI filesystem; these entries are checkout-generated and are not dpath claims. Documents with dpaths do not also appear in `/by-id` in the existing v1 rule. The checkout state directory (`.dtree`) and, when present, node state (`.dnode`) are also checkout-owned surfaces. Their literal names win over user claims *within that checkout*; those claims remain valid in Daybook and must receive other usable materialized names if selected.

A checkout can filter claims by a query or prefix. It may choose to hide `/trash/**` without rewriting or forbidding those claims. A selected but inaccessible document, missing blob, missing target facet, or unrenderable facet is not silently treated as an empty ordinary file. The CLI or application reports which content is pending or unavailable; missing blob bytes are not fabricated as filesystem stubs. The checkout must not treat its own generated surfaces or nested checkout metadata inside adopted directories as new files to re-import recursively.

A lens maps selected content to an **entry set**. One claim may produce a note plus sidecars or a thread plus attachments, rather than exactly one file. Internal names in that set are a lens concern; collisions between outputs from different claims are a checkout naming concern. The full-document JSON view is a useful baseline lens, while text and blob representations are the basic import/render paths. Multiple possible lenses and lens-customization facets require an explicit selection policy, not a promise that extensions alone always choose correctly. This FDR does not declare a file whose editable contents depend on several documents to be supported.

## Bindings and collisions

A **binding** is the checkout's recorded association between a projected output and a real path. The dpath is an address in Daybook; the binding is local correspondence. This history is durable checkout state, not a cache whose loss can always be healed by rescanning the current tree. Once an output owns a path, the arrival of another claimant does not turn the incumbent's file into a directory or rename it. The newcomer takes a distinct name. A checkout with different prior bindings may choose a different clean-name winner; this is presentation divergence, not divergence in document identity or content.

A fresh checkout allocates names from the complete claim set it currently knows, not from the order in which a scan happens to visit entries. Checkout-owned surfaces take their literal names first. Literal dpath claims take precedence over newly derived spill directories, while competing literal claimants use a stable identity-based tie-breaker; the exact claimant ordering and derived-name spelling belong in the naming ADR. Once recorded, a binding is sticky. A later claim cannot evict it, even if allocating the now-complete set from scratch would have produced different names. Different arrival histories may therefore produce different local names without changing document identity.

At least these collision classes must work together:

| Selected content | Required user-facing behavior |
| --- | --- |
| A alone claims `/inbox/hello.md` | Expose A at `/inbox/hello.md` when the filesystem permits it. |
| A is bound at `/inbox/hello.md`; B later claims it | Keep A there; expose B under a distinct derived name, preserving a useful extension where applicable. Do not replace A with a directory. |
| A is a file at `/a`; another claim needs `/a/b` | Preserve A's file; place the descendant under a distinct usable directory, such as `/a.d/b` if that name is free. |
| A claims `/a.d` but nobody needs descendants under a file at `/a` | Do **not** invent a spill: A may use `/a.d`. |
| Both `/a` as a file and `/a/b` need a spill, while another claimant owns `/a.d` | Preserve existing bindings; resolve the spill/literal-name collision without losing either claimant. Exact suffix iteration is not fixed here. |
| Fresh checkout sees `/a` (file), `/a.d` (file), and `/a/b` together | Give the literal files `/a` and `/a.d`; use a distinct `.d`-marked directory, such as `/a.d.d/b`, for the child. The scan order must not change this allocation. |
| Checkout has already bound `/a/b` under `/a.d/b`; D later claims literal `/a.d` | Keep the existing `/a.d/` directory; give D a distinct file name. A later claim does not reallocate earlier bindings. |
| A claims `/by-id/x` in a checkout with `/by-id` | Keep `/by-id` for the checkout surface and give A a distinct real path; do not prohibit its Daybook claim. |
| Two names are distinct in Daybook but equal on the target filesystem | Preserve established bindings and distinguish the names on disk or report that safe representation is unavailable. |

The old rule that made *every* many-claimant dpath into a directory contradicts stable bindings and is rejected. The example `A → /a; D → /a.d` alone is not a file/directory collision. `.d` is the recognizable preferred marker for a directory spilled from a file/descendant collision, not a reserved Daybook suffix: literal `.d` claims remain valid, and a lens may deliberately produce a directory, including an empty one. Spill directories are checkout outputs for displaced children, not extra claims made by the file at their original path. Stacking `.d` is a candidate when a spill name is already occupied; its interactions with literal suffixes, lens outputs, and platform normalization need a terminating algorithm in an ADR. The FDR guarantees no silent loss, deterministic initial allocation over the known set, and no later-arrival rename of incumbents—not one universal name independent of arrival history. If the clean-name incumbent disappears, others keep their existing derived paths until explicitly rebound.

## Working with files

A filesystem scan observes a state, not necessarily the sequence of operations that produced it. The Daybook backend uses recorded output bindings and last-rendered document heads to interpret changes; the generic tree and filesystem backend do not need to know Daybook IDs. Better filesystem move heuristics may improve recognition later without making inode identity part of a document or dpath.

| User action | Checkout expectation |
| --- | --- |
| Edit a rendered file | Send its edit to the owning lens, using the version that produced the previous render as its base. Do not silently overwrite unreported local bytes with a remote update. |
| Create a file | Import using an applicable lens, with basic text or blob handling as the fallback rather than silently ignoring the file. A specialized lens failure must not discard the source bytes. |
| Move a rendered file | If its origin can be identified, retain its document identity and update its address through the Daybook/lens operation. A filesystem move is not evidence that two documents are one. |
| Remove one file from a multi-file output | Deliver the removal to that output's lens. It can update the owning document if the edit is representable; it cannot silently delete the whole document. |
| Remove the sole output of a document | Interpret removal of that projection/claim; do not automatically delete the document from the node. |
| Remove A's file at `x` and move B's from `y` to `x` | If observations distinguish the operations, remove A's relevant output and move B's claim, even when they belong to different documents. The final name `x` alone does not make B an edit to A. |
| A remote document update arrives during local edits | Keep the local work safe and interpret it against the render base; document acceptance and semantic bounce belong below the checkout. |

When two identical files and their observed metadata leave the history ambiguous, **the existing binding at the path wins**. The system may therefore interpret a real move as a deletion. It must not claim perfect move detection from snapshots or change document ownership just because two hashes match. The CLI needs to show consequential ambiguity and allow deliberate correction of correspondence; precise controls belong in the CLI FDR.

Deleting a checkout file **never automatically deletes its Daybook document**. Document lifecycle management requires an explicit operation; the CLI should allow finding documents once projected in this checkout. Removing a dpath facet stops projecting that address; it does not erase the underlying history or revoke other claims. If a blob existed only at the deleted checkout path, its bytes may genuinely be lost: neither a retained document ID nor a digest is a backup. Checkout deletion and remote removal must not destroy locally changed bytes just because the target was previously claimed by Daybook.

Local filesystem changes may touch several documents. The checkout can apply its own state changes transactionally, but it cannot promise a single Automerge head or one atomic replicated commit across those documents. Normal filesystem use must not require collection-wide version-control machinery. Rich CLI operations can present the separate document writes and their outcomes without inventing a global commit identity.

## Unavailable content and document conflicts

A dpath claim may exist while its blob is absent, a cross-document target cannot be resolved, or its current document view cannot be rendered. These are different states for status and recovery. Missing content or a missing target is not a zero-byte file. A missing blob, including one lost after in-place adoption, must be reported honestly rather than implied to be retained. A local filesystem deletion of an adopted path is a removal, not automatically a missing-blob stub.

The generic tree and filesystem checkout do not classify Daybook semantic conflicts. The document/lens write boundary determines whether an edit can be accepted or must be preserved via the bounce policy. A runtime failure or unavailable input is not a semantic bounce. Invalid content already received from another node cannot be moved retroactively out of its history; the document layer must decide which state is renderable and how it can be repaired. Checkouts do not write conflict-marker text into otherwise ordinary files. The detailed policy for writes against already-invalid heads belongs in a document-layer ADR; this FDR does not invent its answer.

## Adoption and import

Adoption begins with an existing directory. A track-only adoption can record local candidate paths without creating documents or dpath facets; these names become Daybook claims only on import. An Obsidian vault can then use its existing note paths, and a DCIM library can use `/DCIM/...` paths, without requiring a rename solely for Daybook. Import creates or selects document identities and chooses lenses. The normal one-file/one-document choice and the one-document/many-files choice are both useful: the latter lets a collection of related source files share one document history while receiving individual path claims. The facet mapping and CLI choice for this mode still need specification.

Media adoption should not promise an extra full copy where a safe backend strategy can reuse existing bytes, but a hardlink to a mutable checkout is not safe merely because its source once appeared immutable. Byte transfer and retention guarantees need the blob/backend ADR and honest status. Imported claims face the ordinary collision policy; there is no secret digest-equality takeover rule. Re-import into an already known document can reuse the same path-keyed claim, while two devices independently inventing document IDs can produce distinct claimants and a collision.

## Decisions still required
!> it keeps it's path
1. **Names:** what are the exact derived-name and spill rules for claimant collisions, lens entry sets, file/child collisions, reserved surfaces, literal suffixes, case folding, and forbidden platform names? Prove termination and preservation of existing bindings before calling the algorithm fixed.
!> implement a quick typescript version in ./docs/scratch/
2. **Import encoding:** how are non-UTF-8 filesystem names represented reversibly, or when does import refuse them? Percent-encoding a facet key is not automatically a solution: its decoded dpath identity, literal `%` names, and URL encoding must remain unambiguous.
!> should we require url encoding for keyids? I think that might make sense
3. **Lens scope and selection:** how are cross-document target references rendered without creating one editable output with several histories? How does Daybook choose facets and a lens for each claim, including whole-document claims selected through the Body facet, and what is the fallback when a specific lens does not apply? What happens to an unrepresentable partial deletion? These require a worked lens ADR.
!> this is a good quesiton and highlights our poorly designed lens system. i.e. how does hte daybook backend select document facets and select the lenses and so forth? keep this open quesiton, we'll discuss it in the ADR
4. **Import granularity and preferred display:** how does a user choose one document per file versus one document for several files? A whole-document claim uses the Body facet to select its primary lens; when several dpaths can be shown as the document's preferred address, what ordering or user choice applies?
!> body facet is our canonical "this is the main facet of a document" system. for dpaths that refer to wholee documents for example, that's how we'll select the lens. indeed, this means we need a sophisticated lens selection -> fallback system at both ends! A big missing detail from the prev ADR
5. **Recovery UX:** what CLI operations expose old bindings, correct ambiguous identity, and explicitly manage a document that no longer has a projected path? The detailed commands belong in the CLI FDR.
!> agree
