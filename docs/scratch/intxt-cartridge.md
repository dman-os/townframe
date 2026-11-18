# intxt as one cartridge (ADR 047 + ADR 049)

Status: Proposal, for the operator's review
Owner: intxt lane
Updated: 2026-11-18
Scope: How intxt's nodule and the review micro become **one cartridge** under ADR 049 (one permission model for nodules and micros): its permbinds, each part's `uses`, what is granted and when, what is requested at runtime, and how it is packaged and installed. It replaces the interim pieces the phone run used (the micro's `docCap` config and `interimMethods` list, and the install's per-operation grants). Base: nodules' stack at node-47 (per-cartridge grants #182, runtime grant requests #184).

## Today

- **The nodule** is a `FixtureCartridge` (`intxt_core::cartridge`) with three routines (`createIntxt`, `addToIntxt`, `summarizeIntxts`). Its permbinds are only `#method`, one per operation it calls (`readDatums`, `writeDatums`, `mintDocCapability`, `createDrawer`, `addDoc`, `listDocDrawers`, `mintDrawerCapability`), and each routine's `uses` lists its subset. The routines mint their own doc capabilities (`mintDocCapability`) to reach drawers.
- **The micro** (`micro_review`) is a separate package. It gets its reach from an install-time config datum (`githubDrawer`, `intxtDrawers`, `docCap`: a holder-bound capability the phone composition mints) and an `interimMethods` list in its manifest's `extra`.
- **Install** (`intxt_core::install`) publishes the nodule's cartridge, then grants each operation one by one (`SetCartridgeGrant` per `REQUIRED_OPERATIONS`). It signs with the fixture key (`sign_as_example`).

## Target

### One cartridge, two runtimes

`org.example.intxt`, one version, with:
- **Lexicons:** the intxt datums (generic, work, annotation), as today.
- **Components:** `handler` (the nodule wasm) and `review` (the micro wasm).
- **Routines:** `createIntxt`, `addToIntxt`, `summarizeIntxts`, as today.
- **A micro contribution** `review`, holding the review micro and its `uses` (ADR 049 §3).

The two runtimes share one permbind list and one set of grants. Each keeps to its own `uses`, which a grant never widens, and neither gets implicit access to the other (§8: no cartridge-level parent). The micro calling its own cartridge's `summarizeIntxts` is still a declared `#method` in its `uses`; the person isn't asked separately for it (§3, asymmetry 5).

### Permbinds (the cartridge's holes)

The default shape is a datum kind in a drawer (§2).

| Permbind | Kind | What |
|---|---|---|
| `intxts` | `#drawers` | create, read, write: the drawers intxt makes (one per intxt) |
| `intxtRecords` | `#datum` | the generic and work datums (marker, title, body, entries, gitCommit, rewrite, rewrittenFrom, githubPullEntry, commitsDoc): read and write, in `intxts` drawers |
| `annotations` | `#datum` | ADR 041's comment and thread datums: read and write |
| `pullRequests` | `#drawers` + `#datum` | read `githubPull` in the node's GitHub drawer |
| `contentBlobs` | `#datum` | read and create `org.example.daybook.blob` (packs and the diff blob) |
| docs, drawers and blobs operations | `#method` | `readDatums`, `pollDatums`, `writeDatums`, `createDrawer`, `addDoc`, `listDocDrawers`, blob reads |

No `#origin`. GitHub isn't raw fetch for intxt (§2, operator review item 5). Today the CLI fetches with `gh`. Later a GitHub platform cartridge provides operations (that's where the personal access token the operator chose lives), and intxt declares a `#method` on them.

### `uses` (each part's ACL)

| Part | Uses |
|---|---|
| `createIntxt` | `intxts` (create), `intxtRecords` (write), `createDrawer`, `writeDatums` |
| `addToIntxt` | `intxtRecords` (read, write), `contentBlobs` (read), `addDoc`, `readDatums`, `writeDatums` |
| `summarizeIntxts` | `intxtRecords` (read), `readDatums` |
| micro `review` | `pullRequests` (read), `intxtRecords` (read), `contentBlobs` (read), `pollDatums`, `readDatums`, `summarizeIntxts` |

The routines stop minting their own doc capabilities. Under §6.1 the runtime mints each routine's handles from `uses` ∩ grants, so `mintDocCapability` and `mintDrawerCapability` leave the manifest. The narrowness is the point: §9a's analysis can then say "the review micro reads PRs and intxts and sends nothing outward".

### Grants (per cartridge, in the person's nouns; §4.1)

- **At install** (proposed, then accepted): the operations as `grant/method/…`; the node's GitHub drawer as `grant/drawer/<github drawer>` `{read}` ("may intxt read your imported pull requests?"); and the intxt drawers already on the node as `grant/drawer/<id>` `{write}`.
- **What intxt creates itself**: whether a drawer the cartridge created through `intxts` (create) is reachable without a grant datum is a question for nodules.
- **At runtime** (§4.2), when the micro opens an intxt the cartridge holds no grant for, typically one synced from another node, which is the phone's normal case: it files a request for `grant/drawer/<that intxt>` `{read}`, shows "intxt needs to read 'install as a library call'", and awaits the answer. Allow (or "only this time") lets it read; decline shows the intxt as unreadable for the rest of the boot.
- **`MissingAuthority`** (§4.3): a call the cartridge isn't granted surfaces as a typed row ("intxt needs permission for this"), not as "unreadable" with an error string.

This retires the micro's config datum (`githubDrawer`, `intxtDrawers`, `docCap`) and `interimMethods`. The GitHub drawer becomes a grant, and the micro finds it by a datum in the drawer or by the grant itself, not by config.

### Packaging and install

- **One cartridge version** with both components, staged and published as one. Each component's bytes are a blob, and the micro's description comes from its Clay manifest (`big_cartridges/import.rs` is the path for a micro contribution; to confirm with nodules and claytxt).
- **`intxt_core::install`** publishes that version, proposes the install grants above (no more per-operation `SetCartridgeGrant`), and enables it. It stays idempotent per version. The phone installs the same cartridge at boot, so the micro and the nodule always match in version.
- **Signing** stays the fixture key until the operator settles the namespace and publisher DID (open).

## Order of work

1. Restack intxt onto node-47 and adapt the install to per-cartridge grants: what's there now.
2. Permbinds and `uses` as above. The routines stop minting. Tests: the cartridge test asserts each part's `uses` against the table, and each routine's e2e runs under its derived handle.
3. The micro as a contribution of the cartridge, with grants instead of config: the phone and web composition drop the interim config.
4. Runtime requests and `MissingAuthority` in the micro (needs §4.3 on the node, and claytxt's host service for requests).

## Open

1. Does a cartridge reach the drawers it created through a `#drawers` create permbind without a grant datum, or does creation also write `grant/drawer/<id>`?
2. How does the micro find the node's GitHub drawer once there's no config: a well-known datum the CLI writes, or by listing the drawers the cartridge is granted?
3. Is `#datum` narrowing inside granted drawers wired at node-47, or only in the ADR? If only in the ADR, the first version declares the kinds and enforcement follows.
4. How the micro's bytes and description enter the cartridge version: the import path, or a component datum that intxt's install writes itself.

## Answers from nodules (2026-11-18, code at node-47/48)

- **No authoring builder yet** (§2's surface isn't built). intxt writes the ADR 047 manifest datums directly (the Cartridge envelope, permbinds, routines and contributions with `uses`), as `FixtureCartridge` and `example_cartridge!` lay them out, in **one function** in intxt_core, so nodules' builder can replace it later without touching the rest. Shapes the builder would need go to nodules.
- **Packaging:** `big_cartridges/import.rs` is Clay's stand-in importer. It turns one micro package into a cartridge whose only contribution is that micro, and it doesn't merge. intxt publishes **one envelope with both contributions** itself (`PublishCartridgeVersion`). Whether the micro host reads a micro contribution out of a cartridge that also has a nodule is claytxt's to confirm.
- **`MissingAuthority`** is node-48: `method_core::ContextError::MissingAuthority { cartridge, permbind, request }`, decoded from every method's error. node-49 adds `day drawers remember` and `listenPort`. intxt restacks onto node-48 or node-49 once pushed (swarm log).
- **Grants** are `grant/drawer/<id>` per cartridge, so one grant covers the nodule and the review micro. Runtime requests are `requestCartridgeGrant`, answered with `day grants accept`.

Open item 4 is answered (publish both contributions ourselves). Items 1–3 stay open.
