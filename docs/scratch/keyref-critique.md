# #keyRef and x-content-hash: the operator's objection, a steelman, a narrower shape

Status: Discussion, for the operator and the nodules lane
Owner: intxt lane
Updated: 2026-11-17
Scope: ADR 035 §2.9 (#keyRef, `x-content-hash`), landed on -next as be159fca..24c3211c. intxt's use of it (feat/intxt-keyref-entries, unpushed) waits on this.

## The objection (operator, 2026-11-17)

- A reference that names no document implies a global index. As a programming primitive it is easy to misuse.
- A hash as the key makes the datum immutable. For mutable datums every write would be a new datum, which leaves garbage.
- A search API for datum keys would do; a specific "keyRef" meaning in the sidecar is a footgun.
- `x-content-hash` widens what the sidecar means: it is a rule about identity, not about how a value is stored.

## Steelman

1. **Mutable datums can't use it.** A keyRef is admitted only to a kind that declares `x-content-hash`. Such a kind refuses any value that doesn't hash to its slot, so it can't be edited: new content is a new key, and the old one is untouched. Mutable datums keep document-scoped references. Declaring a mutable kind content-hashed would make every edit fail; it would not pile up garbage.
2. **The identity exists outside the system.** A git commit is its sha in GitHub, in review comments, in reflogs and on every machine. Copies live in many documents: every node's repo document, and possibly intxts. Each node creates its own repo document. Its slot comes from the root commit, but its document id differs per node. So my intxt points at `<my repo doc>/gitCommit/X` and yours at `<your repo doc>/gitCommit/X`, and a document-scoped "who holds X" sees two different datums. Only a key-scoped reference brings them together.
   **Narrowed by the nodules lane (2026-11-17):** the divergence comes from intxt creating one repo document per node (a document id is minted fresh at creation, big_repo/lib.rs:1047). If a user's repo document is created once and lives in a drawer all their nodes share, every node holds the same document, and document-scoped references converge without key references. A key reference is then needed only across drawers or users: two people's intxts over the same commit, or an intxt pointing at a commit held in someone else's repo document. For D62 as scoped (my PRs, my intxts, on my phone), a per-user shared repo document would be enough, and #keyRef only pays off for cross-user review.
3. **The hash check makes "any copy" safe.** Without it, "read X from whichever document has it" lets anyone plant a forged `gitCommit/X` in a document you can read. With it, every copy is verified against its key.
4. **The index isn't global.** `QueryDatumRefs` already indexed document-scoped references; `Key` is one more selector over the same per-node index, limited to documents the node holds and the caller can read.

## Where the objection lands

- **The primitive is broader than the need.** The only content-addressed kinds are git objects (and blobs, in tests). What intxt needs is narrower: find the datums that mention commit X, across the documents I can read.
- **A key search alone doesn't answer it.** Searching datums by key finds the commits, not the intxts holding them. Without some reference the node understands, it would have to index arbitrary fields in values (worse), or intxt keeps its own membership index (which needs its own convergence across nodes).
- **`x-content-hash` does widen the sidecar.** Until now the sidecar described layout and storage (`x-automerge`). This adds an admission rule tying the slot to the value. Lexicon constrains record keys (`tid`, `nsid`, `literal:self`, `any`), and "the key is the content hash" is a key type in that spirit. It lives in the sidecar only because Lexicon has no extension point for key types.

## A narrower shape

1. **A key type, not a reference type.** The kind declares its key is a content hash (git object, commit), next to where key types are declared, or in the sidecar named as a key type. Admission checks it as today.
2. **No new reference member in values.** Entries hold the plain key text, `gitCommit/<sha>`, in a field the kind's schema marks as "a key of a content-addressed kind". Or entries hold shas, and the kind names which content-addressed kind they key.
3. **One query: "datums that mention key K".** It is restricted to keys of content-addressed kinds, so the index can't grow over mutable data. It is scoped as `QueryDatumRefs` is today: this node, the caller's capabilities, optionally one drawer.
4. **A read by key from any reachable copy** (today's `ResolveKeyRef`), also restricted to content-addressed kinds.

The nodules lane checked this against node-22 and leans the same way: keep the query and the read by key (the substance), move "content-addressed" to a key-type declaration, and replace the `#keyRef` value def with a field-level schema marker. node-10 isn't merged, so reworking it now is cheap. They change nothing until the operator decides.

Points 3 and 4 are what #keyRef does today. The difference is that "this field names content by hash" is a property of the field's schema, not a reference type every datum author can use. Whether that difference is worth reworking landed code is the operator's call with the nodules lane.

## intxt's position

Either shape serves D62. feat/intxt-keyref-entries (e7dc15a4) stores the entries' commits as #keyRefs. If the narrower shape is chosen, the field type changes and the shas stay. The branch stays local until this is settled. The PR description link (`dtxt:datum:<drawer>/org.example.intxt/main`) doesn't depend on either.
