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
2. How does the micro find the node's GitHub drawer once there's no config: a well-known datum the CLI writes, or by listing the drawers the cartridge is granted? **Answered 2026-11-18 (operator, ledger D67): neither.** The micro reads through `clay_docs` queries, which the host serves under its permissions: the PR list queries the PR kind over what the cartridge is granted, and an intxt reaches its other drawers as children.
3. Is `#datum` narrowing inside granted drawers wired at node-47, or only in the ADR? If only in the ADR, the first version declares the kinds and enforcement follows.
4. How the micro's bytes and description enter the cartridge version: the import path, or a component datum that intxt's install writes itself.

## Answers from nodules (2026-11-18, code at node-47/48)

- **No authoring builder yet** (§2's surface isn't built). intxt writes the ADR 047 manifest datums directly (the Cartridge envelope, permbinds, routines and contributions with `uses`), as `FixtureCartridge` and `example_cartridge!` lay them out, in **one function** in intxt_core, so nodules' builder can replace it later without touching the rest. Shapes the builder would need go to nodules.
- **Packaging:** `big_cartridges/import.rs` is Clay's stand-in importer. It turns one micro package into a cartridge whose only contribution is that micro, and it doesn't merge. intxt publishes **one envelope with both contributions** itself (`PublishCartridgeVersion`). Whether the micro host reads a micro contribution out of a cartridge that also has a nodule is claytxt's to confirm.
- **`MissingAuthority`** is node-48: `method_core::ContextError::MissingAuthority { cartridge, permbind, request }`, decoded from every method's error. node-49 adds `day drawers remember` and `listenPort`. intxt restacks onto node-48 or node-49 once pushed (swarm log).
- **Grants** are `grant/drawer/<id>` per cartridge, so one grant covers the nodule and the review micro. Runtime requests are `requestCartridgeGrant`, answered with `day grants accept`.

Open item 4 is answered (publish both contributions ourselves). Items 1–3 stay open.

## Example envelope (for claytxt's cartridge-micro loader fixture)

ADR 047 §2's shape, extended by ADR 049 §3 with a `micro` contribution beside `nodule`. The `micro` member's exact fields are claytxt's to fix (Clay on the node §5). What intxt relies on: the contribution names its component by key, carries its Clay manifest as a pinned datum, and declares `uses` over the cartridge's permbinds like a routine does.

```jsonc
{
  "$type": "org.example.daybook.cartridge",
  "id": "did:key:…/intxt",
  "version": "0.2.0",
  "authority": "org.example.intxt",
  "name": "intxt",
  "lexicons":   { "org.example.intxt": { /* #datumPin */ }, "org.example.intxt.work.gitCommit": { /* … */ }, … },
  "sidecars":   { "org.example.intxt.work.gitCommit": { /* #datumPin */ }, … },
  "components": { "handler": { /* #datumPin: the nodule wasm */ }, "review": { /* #datumPin: the micro wasm */ } },

  "permbinds": {
    "intxts":        { "$type": "…#drawers", "description": "The intxts it creates",
                       "create": "required", "read": "required", "write": "required" },
    "pullRequests":  { "$type": "…#drawers", "description": "Your imported pull requests",
                       "read": "optional" },
    "intxtRecords":  { "$type": "…#datum", "nsid": "org.example.intxt", "description": "Intxts: titles, entries, commits",
                       "read": "required", "write": "required", "create": "required" },
    "githubPulls":   { "$type": "…#datum", "nsid": "org.example.intxt.work.githubPull", "description": "Pull requests",
                       "read": "optional" },
    "contentBlobs":  { "$type": "…#datum", "nsid": "org.example.daybook.blob", "description": "Packs and diffs",
                       "read": "required", "create": "required" },
    "readDatums":    { "$type": "…#method", "nsid": "org.example.daybook.docs.readDatums",  "necessity": "required" },
    "pollDatums":    { "$type": "…#method", "nsid": "org.example.daybook.docs.pollDatums",  "necessity": "required" },
    "writeDatums":   { "$type": "…#method", "nsid": "org.example.daybook.docs.writeDatums", "necessity": "required" },
    "createDrawer":  { "$type": "…#method", "nsid": "org.example.daybook.drawers.createDrawer", "necessity": "required" },
    "addDoc":        { "$type": "…#method", "nsid": "org.example.daybook.drawers.addDoc", "necessity": "required" },
    "summarize":     { "$type": "…#method", "nsid": "org.example.intxt.summarizeIntxts", "necessity": "required" }
  },

  "nodule": {
    "routines": {
      "createIntxt": {
        "impl": { "$type": "…#componentExport", "component": "handler", "export": "handle" },
        "implements": "org.example.intxt.createIntxt",
        "uses": { "intxts": { "create": true, "write": true }, "intxtRecords": { "write": true, "create": true },
                  "createDrawer": {}, "writeDatums": {} }
      },
      "addToIntxt": {
        "impl": { "$type": "…#componentExport", "component": "handler", "export": "handle" },
        "implements": "org.example.intxt.addToIntxt",
        "uses": { "intxts": { "read": true, "write": true }, "intxtRecords": { "read": true, "write": true },
                  "contentBlobs": { "read": true }, "addDoc": {}, "readDatums": {}, "writeDatums": {} }
      },
      "summarizeIntxts": {
        "impl": { "$type": "…#componentExport", "component": "handler", "export": "handle" },
        "implements": "org.example.intxt.summarizeIntxts",
        "uses": { "intxts": { "read": true }, "intxtRecords": { "read": true }, "readDatums": {} }
      }
    }
  },

  "micro": {
    "component": "review",
    "manifest": { /* #datumPin: micro_review_manifest's Clay manifest (dayframe.review) */ },
    "uses": { "pullRequests": { "read": true }, "intxts": { "read": true },
              "githubPulls": { "read": true }, "intxtRecords": { "read": true }, "contentBlobs": { "read": true },
              "pollDatums": {}, "readDatums": {}, "summarize": {} }
  }
}
```

Notes:
- `summarize` is the micro's call to its own cartridge's routine: declared, never asked separately (ADR 049 §3, asymmetry 5).
- `pullRequests` and `githubPulls` are optional: without the GitHub drawer granted, the micro still opens intxts it's given, and shows "not granted" for the PR list (claytxt: no request host service yet, so it shows "not granted" until there is one).
- `#datum` with `nsid: "org.example.intxt"` stands for intxt's own record family. If a `#datum` must name one kind each, it becomes one permbind per kind (the cartridge test generates them from `UNIVERSE`).

## From claytxt (2026-11-18)

- No host loads a micro out of a cartridge yet (all load from package sets). Clay on the node §5 "cartridge micros" (a typed `MicroContribution`, a host loader from the node's catalog that resolves `component`) is one of tonight's lanes, and intxt's envelope is its first consumer.
- The micro handle from `uses` ∩ the cartridge's grants is also tonight's lane. Until it lands, review.json (`INTERIM_GRANTS`, docCap) keeps working, and claytxt removes it after.
- The micro side of `requestCartridgeGrant` (a host call the micro awaits, with the prompt drawn by the shell) is claytxt's, later, since the prompt UI needs the operator. Until then the micro shows "not granted".

## The micro member, as settled by claytxt (2026-11-18, feat/cartridge-micros f2d03a27)

`micro: { "component": "<key in components>", "schema": "org.example.clay.micro.manifest", "description": { <the Clay manifest without id, package_version and authority> }, "uses": { "<permbind>": { read/write/create/delete booleans } } }`. A `#method` use is `{}`. There is no `manifest` pin: the Clay manifest is inline in `description`. Its `id` is the cartridge's name, `package_version` is the envelope version's major (0.x → 0, an open operator point), and `authority` is the envelope's. The example's `uses` is admissible if each used access is declared by its permbind. The phone switches to the cartridge micro on its own once intxt's cartridge carries `micro`. Until then, review.json and docCap keep working. Still open (operator): how the micro gets its docs handle (attached by the host on each call, or the interim docCap datum).

## Built at node-52 (2026-11-18, intxt-25-cartridge)

- **Open 1 is answered: no.** A cartridge does not reach a drawer it created through a `#drawers` create permbind. A create use sets only the drawer handle's `create` flag (`big_nodule/state.rs` `desired_routine_scopes`, the `create_drawer |= usage.create` line), and every drawer a handle names comes from `grant/drawer/<id>` datums that exist when the handle is minted (the `desired_grants` filter just above it). The docs handle's reach is likewise a fixed `BTreeMap<DocId, DrawerReach>` built from those granted drawers (`routine_docs_scope`), so a drawer made after the mint is in no handle, and nothing re-mints on creation. The drawer is owned by `caller.originating_actor` (`big_cabinet/drawer/create.rs` `create`), which is a keyhive fact, not a handle scope. The e2e `a_create_only_drawers_use_makes_drawers_but_writes_no_existing_one` pins the create-only half.
- **The routines still mint.** The handles are reachable from a guest (`Cx::config_capability`, `Cx::drawer_capability`, `Cx::blobs_capability`), but `createIntxt` writes the new drawer's descriptor, which no handle names (above), and `addToIntxt` reads a part's document in a drawer the cartridge was never granted and lends it through `listDocDrawers` and a drawer capability over a drawer it was not granted either. So `mintDocCapability` and `mintDrawerCapability` stay as interim `#method` permbinds, and the routines keep acting as their caller.
- **Permbinds as built:** `#drawers` `intxts` and `pullRequests`; one `#datum` per kind a routine touches (`intxt`, `title`, `body`, the four `*Entry` kinds, `worktree`, `gitRepo`, `agentSession`, `githubPull`) and `blob`; `#method` `readDatums`, `writeDatums`, `createDrawer`, `addDoc`, `listDocDrawers` and the two interim mints. `pollDatums`, the annotation kinds and the capture-side kinds (`gitCommit`, `rewrite`, `rewrittenFrom`, `commitsDoc`) are not declared: nothing in the nodule uses them, and they join with the micro.
- **Install** grants each method permbind as `grant/method/<nsid>` and each drawer the caller passes, and grants the ones missing when run again. `day x intxt install` passes none yet: with a drawer granted, the `docs` and `drawers` handles minted for `addToIntxt` and `summarizeIntxts` are refused (`Denied`, instrumented in `mint_missing_handles`), because the `nodules` service actor holds none of the operator's drawers (ADR 049 §6.1: it must hold every right it hands down; the big_nodule e2e grants it the drawer first). The CLI passes the node's intxts at Write and its GitHub drawer at Read once that lands.

## The drawer document (nodules, 2026-11-18)

A drawer reach (`DocScope.drawers`) covers the drawer's member documents, but not the drawer's own document, which is where an intxt's descriptor lives (marker, title, body, entries). Nodules confirms this is a gap and will fix it: a reach with `access >= Read` also reads the drawer document at the reach's kinds, so `grant/drawer/<X>`-derived handles cover it with nothing extra. Writes go to the operator as nodules' proposal: a Write reach writes the drawer document at the reach's write kinds, and system datums stay with the drawers service. intxt needs that, since `createIntxt`, `addToIntxt` and `edit` write the descriptor's kinds. Until then, intxt and the phone's interim docCap list the drawer document in `DocScope.read` (and `write`).

## Runtime handles: two node-side gaps, for the operator (nodules, 2026-11-18)

At node-52, intxt's routines keep minting their own capabilities (interim `mintDocCapability`/`mintDrawerCapability` permbinds, #197) because of two gaps nodules confirms and will put to the operator, with intxt as the use case:
1. **A drawer the cartridge creates is unreachable by its handles.** A `#drawers` create use only sets the create flag, reaches come from the `grant/drawer` set at reconcile, and creating writes no grant. Nodules' proposal: the runtime writes `grant/drawer/<new>` at the permbind's declared access in the same step as the creation, and the reconcile re-mints. The create permission is the consent.
2. **A granted drawer fails to mint.** Handles are minted from the `nodules` (and `micros`) service actor's identity, which must already hold the drawer (049 §6.1). The options are (a) the installer or operator adds the service actor to the drawer before or with the grant, using today's membership calls, or (b) `SetCartridgeGrant`/Allow delegates the person's drawer authority to the service actor in the same call. The hard case is a grant that syncs in from another device, since the service actor is per node.

intxt switches to runtime handles once both are settled.
