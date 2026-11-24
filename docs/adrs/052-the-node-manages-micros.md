# ADR 052: The node manages micros: a `micros` nodule in the core cartridge, `micros.handles`, and one node SDK

**Status:** Draft (2026-11-24). Proposed by the nodules lane and agreed in shape with the claytxt lane; the operator signs
off. Nothing here is built. Decisions marked **To confirm** need the operator.

**Amends:** ADR 049 §6.1 (the `micros` service actor is the `micros` subsystem actor the node opens at boot and hands to the core cartridge's nodule (§2), no longer a
composition the GUI opens; the "same holds for the `micros` service actor" line becomes this ADR) and ADR 050 (§3's micro
surface gains its node-side host; the micro manifest is unchanged). It needs no further change to the authority service's
`openActor` (§2.2).

**Who does what.** The node *manages* micro installs and authority: it opens each micro's actor, mints the handles the
micro may use and re-mints them when grants move. Clay, the GUI's host, *hosts and runs* the micros. The micro wasm runs in
the GUI process, never on the node; the node never executes a micro. Where this ADR says "the host" it means Clay in the
GUI.

Revised 2026-11-24 on the operator's decision "let us indeed try to make the micros layer itself a cartridge". The first
draft put the service in native node code.

Related: ADR 053 (the core cartridge, where the nodule lives), ADR 044 (cartridge configuration documents), ADR 045 (method routes; the operator route is the precedent for a
privileged call), ADR 037 (holder-bound capabilities), ADR 042 §3 (local-only methods and origins), ADR 049 (one
permission model), ADR 051 (guest files), D83 (the node client SDK) and D84 (standalone product binaries such as
`day-intxt`). The ledger entries are `docs/scratch/nodule-runtime-ledger.md` (D56, and its "Attach" to-do in the archive).

Source references are to this branch unless marked `claytxt:`, which means `origin/stack/claytxt-55-ledger`.

## 1. Problem

The desktop GUI (`daytxt_native`) boots the node in its own process and does the micro host's work when the node reports
`Booted`. Two things follow.

- **The GUI cannot run while `day` holds the node.** One process holds a node's lock and serves its socket
  (`day_cli/client.rs:1-14`), and `hosted_runtime` refuses the second one: "attaching to a running node is not built"
  (`claytxt: daytxt_native/lib.rs:385`). A person who has `day node start` running cannot open the app.
- **The work duplicates what the node already does for nodules.** For each micro the GUI opens an actor under a `micros`
  subsystem actor it creates itself, mints dispatch, docs, drawers and blobs handles from the micro's `uses` and the
  cartridge's grants, follows `awaitCartridgeGrants` to re-mint (`claytxt: clay_node/lib.rs` `install_one`,
  `open_micro_actor`; `clay_node/cartridge.rs` `follow_grants`), and installs the intxt cartridge and the interim micros
  (`claytxt: daytxt_native/interim.rs`). The nodules service does every one of these for a nodule
  (`big_nodule/state.rs` `ensure_nodule_actor`, `mint_missing_handles`, `apply_nodule_capabilities`) and is the only copy
  that sits next to the grants it follows.

ADR 049 §6.1 already says who mints: the runtime's service actor, as delegator, with the child actor as subject. Today
only one of the two runtimes has such a service. This ADR gives the other one, as a nodule rather than as more native code: the nodules runtime already is the thing that
mints for guests, so the micros layer is one more guest of it.

## 2. A `micros` nodule in the core cartridge

The micros service is a **nodule** (a request handler: a wasm component the `nodules` host calls per request, ADR 047)
that ships in the **core cartridge** (ADR 053). It is not native node code. Its actor is the `micros` subsystem actor, not the
`nodules/<core>` actor the `nodules` service would open for it. Each runtime has one subsystem actor: node root, `micros`,
`micros/<cartridge>`, `micros/<cartridge>/<micro>`. That makes `micros` a sibling of `nodules/<cartridge>`, as a nodule's
actor is `nodules/<cartridge segment>`, and each runtime is trusted to manage the identities in its own subtree.

The mechanism: the node opens `micros` at boot beside `nodules`, `cartridges` and the other subsystem actors
(`big_node/lib.rs`, `open_actor`). It hands that actor to the nodules service as the core cartridge's nodule actor, so for
the core cartridge `ensure_nodule_actor` (`big_nodule/state.rs`) uses `micros` instead of opening `nodules/<core>`. The
micros nodule's own actor is therefore `micros`, it signs as `micros`, and its routines' handles have subject `micros`.
It has to be the nodule's own actor, and not just a handle the nodule holds, because of holder binding: dispatch and the
authority admit a handle only when its holder's actor presents it, so a delegator bound to `micros` would be useless to a
nodule that signs as `nodules/<core>`. For each enabled micro of each installed cartridge, the nodule:

1. **Opens the micro's actor** under `micros` with `openActor { parent, path, rights: USE }` (§2.2). Actor paths grow
   one segment per open (`InvalidActorPath` otherwise), so the nodule opens `micros/<cartridge>` and
   `micros/<cartridge>/<micro>` in turn (a path that already exists opens to the same actor, which
   `big_nodule/state.rs:3104` relies on), each cartridge and micro id encoded into the one segment that
   `store::actor_segment` makes of a cartridge id. The micro's id is ADR 050 §1's `<cartridge id>/micro/<name>`. An actor is
   durable and survives the GUI that asks for it. Today the GUI opens it per boot with root authority.
2. **Mints its handles** with that actor as `subject`, from the micro's `uses` intersected with the cartridge's grants
   (ADR 049 §3, §4.1): dispatch (the operations it binds), docs (`#datum` kinds in each granted drawer), blobs
   (`BlobsScope`, including `drawers`), and drawers when the `uses` reach them. Every handle is minted from a handle the
   nodule itself holds (its own permbinds, §2.2), narrowed with `attenuateCapability` with `subject` set
   to the micro's actor. This is ADR 049 §6.1's "a runtime grants itself first, then its children" with the runtime being a
   nodule, and with no native code behind it. The result has the shape of `RoutineCapabilities`
   (`big_nodule/executor.rs:41`). The lowering from `uses` to scopes is the one the native `nodules` service runs for a
   routine; it moves out of `big_nodule` into a core crate that both the service and the nodule's SDK use, so it is not
   copied.
3. **Composes the micro's config datum**, the record the host puts in the micro's configuration slot. It carries what the
   node knows (today: the document capability under `docCap`, which the GUI splices in
   `claytxt: clay_node/lib.rs` `install_one`). The nodule writes it nowhere: the collection store is Clay's (§9), so the
   host writes the datum the nodule returned.
4. **Mirrors drawers.** A drawer granted to a micro must also be remembered in the known-drawers registry or the micro's
   reads stay empty (`claytxt: clay_node/lib.rs` `mirror_drawers`). That step moves into the nodule unchanged.

Re-minting when grants move is lazy, not a follower: see §3, `awaitHandles`. A nodule is a request handler, so there is no
background task to hold a feed open.

### 2.1 What stays native

The authority service, dispatch, the cabinet (drawers, documents, blobs) and the `nodules` host itself stay native node
code. Something has to hold keys, route calls and host wasm, and none of those can be a guest of itself. The micros layer
is the first surface that is not one of those, which is why it can leave.

### 2.2 What the authority service needs (two changes)

The authority code (`big_cabinet_core/authority.rs:405-482`) says today: `openActor { parent: CapId, path, rights }` opens
a child actor under a held capability; `mintCapability`'s `subject` must be a **strict descendant** of the subject it
inherits, and an introduction needs `DELEGATE` on the parent or issuer; `attenuateCapability` and `retractCapability`
are already unprivileged (the latter only retracts descendants of its `via` handle, `may_retract`). `mintCapability`
is `privileged`, because it mints under a caller-named issuer namespace. Two changes remain, and the first is none:

1. **No further change: `openActor` and `attenuateCapability` already suffice.** Node-78 (#325) made `openActor`
   unprivileged and keyed on a held identity capability: `parent` is a held handle with MINT_CHILD, and each open adds one
   path segment. `attenuateCapability { subject }` introduces strict descendants of the held handle's subject, so the
   nodule narrows its own handles onto each micro's actor with it. `mintCapability` stays privileged and the nodule does
   not use it, which settles the earlier question of pinning its free-form `object`: attenuation narrows from a held
   handle, so there is nothing to pin. A privileged micros subsystem (the operator's alternative) is rejected: the
   subsystem-actor shape needs no new privilege.
2. **An explicit runtime permbind to read other cartridges' installs and grants.** The nodule must read what other
   cartridges are granted (`listCartridgeGrants`, `awaitCartridgeGrants`, both `privileged` today) and which are installed
   and enabled. A routine does not get that from its `uses`. The core cartridge's manifest declares a permbind on those
   methods (ADR 047, ADR 049 §3), a **runtime permbind** because what it reaches is the node's own bookkeeping, not
   another cartridge's collection. It is granted once, at install of the core cartridge, by the node (ADR 053 §3), not by
   a person: the person cannot be asked about the cartridge that asks about the person's cartridges. The permbind covers
   reads only. Nothing in it lets the nodule write a grant.
3. **Lazy re-mint.** Nodules are request handlers, not long-lived, so `micros.awaitHandles` itself long-polls the grant
   feed and re-mints when it moves (§3). There is no background follower, so nothing in the node has to keep a nodule
   alive.

## 3. Methods

All three are in the `org.example.daybook.micros.` namespace. They are the nodule's own methods: dispatch routes the NSIDs
to the core cartridge's nodule like any nodule method (ADR 045), so they are no longer host methods.

```text
micros.handles { cartridgeId, micro? }
  -> { micros: [{ micro, actor, handles: { dispatch, docs, blobs }, config }], revision }

micros.awaitHandles { since: { <cartridgeId>: revision, ... }, holdMs }
  -> { cartridges: { <cartridgeId>: { micros: [...], revision } } }      // only those that moved
```

- **`handles` is what the host fetches the handles it mounts a micro with.** It returns all of a cartridge's micros in one call (or one, when `micro` is given), so booting the GUI is one
  round trip however many micros the cartridge ships. It is idempotent: asking again returns the live handles, it
  does not re-mint. It answers at once and never holds.
- **`awaitHandles` is `awaitCartridgeGrants`' pattern, and it does the re-minting itself.** A revision is opaque and
  names the boot it was taken in. A `since` that is missing or not current answers at once. Otherwise the call holds up
  to `holdMs`, clamped to `AWAIT_MAX_HOLD_MS` (15 s), long-polling the grant feed (`awaitCartridgeGrants`, through the
  runtime permbind of §2.2). When the feed moves, the call re-mints for the cartridges that changed, in 6.4's order: the
  micro is registered not ready, the old handles are retracted, new ones minted (`big_nodule/state.rs:1028`,
  `apply_nodule_capabilities`), the revision advances, and the call returns the new handles. An expired hold answers
  unchanged. The keys of `since` are the cartridges the host is watching, so a host with several cartridges polls once. A
  closed feed (the service stopping) ends the hold as an expiry does (`big_nodule/lib.rs:469`).
- **Lazy, so two edges.** Re-mint happens only while some host is polling. A grant that is *added* shows up at the next
  poll, which is what a host that wants it is doing anyway. A grant that is *revoked* fails closed without a poll:
  retracting it already retracts the capabilities that descend from it, so a stale handle stops working and the poll only
  refreshes. Two hosts polling the same cartridge converge on one re-mint, because the handle revision is the nodule's
  durable state and not a call's local one. If these edges prove unacceptable in the trial, a native follower is the
  fallback (§12 item 1).
- **Nothing blocks on the long poll.** The host mounts each micro with the handles `handles` returned and re-renders the
  mounts of a cartridge when `awaitHandles` reports a new revision. A mount never waits for the poll, and a micro that
  loses a handle sees a retryable not-ready answer between retraction and re-mint (ADR 049 §6.4), never a stale handle.
- **`actor`.** The result names the micro's actor capability too, because dispatch requires it: dispatch admits a call only
  when the capability's holder is the signer's actor (ADR 037; `big_nodule/egress.rs` states the check dispatch runs), and
  the GUI is a separate process from the node, so something outside the node has to sign as the micro. (This only matters
  when `day node start` holds the node in another process and the GUI reaches it over the socket; in process it is the same as
  today.) That is the signer
  the in-process composition holds today (`claytxt: clay_node/egress.rs`, `MicroEgress::install`). It carries `USE` only,
  and it is the host's, never the guest's: the guest names an operation, the host attaches handle and signer (ADR 049 §3,
  asymmetry 2). The actor capability stays in the host and never crosses into the guest: the host overwrites whatever
  `authorization` a guest request carries (`claytxt: clay_node/egress.rs:12`).

**What is privileged, and how the GUI is authorized.** All three hand out capabilities, so they stay `privileged` (only the
operator may ask), as `listCartridgeGrants` is. The nodule recognises the operator by the resolved root capability
(`ResolvedAuthority::is_operator_root`, `big_cabinet_core/authority.rs:302`, as `big_nodule/lib.rs:300` does for the
native service) and nothing it was configured with. Over the holder's Unix socket the kernel's peer credentials make the
connecting process the operator (`big_dispatch/xrpc.rs:44-50`, `Anonymous::Acts`); in process, the GUI's client acts as the
root capability. The GUI is therefore a client with operator authority that mints nothing: the nodule mints, from the
handles of its own actor, and the handles the GUI receives are scoped to each micro and bound to its actor, so a
compromised micro still holds only what its `uses` and the grants give.

## 4. Egress

A micro's `dispatch.local` request is an XRPC call to the holder's dispatch carrying the micro's minted dispatch
capability as the bearer and its actor as the signer. The SDK of §8 carries that XRPC client, and the socket
transport it uses exists today (`day_cli/client.rs` `Transport::Socket`). The in-process transport is today's
`big_nodule::egress` path (`claytxt: clay_node/egress.rs`).

**The socket's origin.** The Unix listener stamps every call `Origin::UnixPeer`
(`big_dispatch/xrpc.rs:101`), which is local, while a guest's egress is `Origin::Remote` by rule (ADR 042 §3: "not a call
relayed through guest egress"). A micro calling over the operator's socket would reach local-only methods that a nodule
guest never could. A capability is still needed, so this is an exposure rule and not an authority leak, but it undoes the
rule. Options:
- **A separate guest-egress listener** on the holder (its own socket path, `Edge::Guest`, origin `Remote`) that admits
  only a presented capability and never `Anonymous::Acts`. A micro's calls always use it.
- A `micros.call` proxy that re-enters dispatch with `Origin::Remote`. It puts every micro byte through one more method
  for no authority gain.

Proposed: the listener, which keeps today's rule rather than adding one (the claytxt lane agrees). **To confirm.** In process the egress already stamps `Remote`, so only the socket path changes.

## 5. What moves out of boot config

The GUI passes the node a `NodeConfig` today (`claytxt: clay_node` `NodeConfig`: `schemas`, `installs`, `bootstrap`,
`serve`). What it carries goes where it belongs:

- **The host's datum vocabulary ships as installed cartridges, not as `schemas`.** Two cartridges: the **core cartridge**
  of ADR 053 (the node's built-in type library, `org.example.daybook.defs`, hardcoded today as `node_type_libraries()`, and
  the micros nodule of §2), and the **daytxt cartridge** of §6. The node then admits the same Lexicons wherever a client
  reaches it. `day` already boots with an empty `SchemaRepo` and no user datums without a cartridge (ledger), so this also
  fixes `day`. Boot order: the core cartridge before every other, and ADR 053 §3 says how.
- **The peer ticket moves into `getNodeInfo`.** It is already there: `PeersInfo.address` is the endpoint ticket
  (`big_node/info.rs:75`, filled at `:147`). So no new field; the GUI stops writing `node-address.txt` into its pairing
  directories (`claytxt: daytxt_native/lib.rs`, around :475) and reads `getNodeInfo` instead, or leaves the file to `day`.
  `address` is `None` when the node has no peers service, so a GUI that wants to be dialable asks `day` (or a settings
  update) for it, as `prepare_settings` does today.
- **Bootstrap tickets stay node settings** (`NodeSettings`, `peers.add_bootstrap`). They are the operator's per-node
  choice, not a GUI's, and `bootstrap.txt` stays the file a person pairs with.

## 6. Micros with no cartridge: the daytxt cartridge

A bundled micro has no cartridge. Its identity, `uses` and grants live in a table in the GUI today
(`INTERIM_GRANTS`, `claytxt: daytxt_native/interim.rs:47`), which the GUI applies as operator. That is the same
authority decision made in a place the person cannot audit.

Proposed: a **daytxt cartridge**, installed with the operator root capability, that declares every micro the app ships,
each with its `uses`:
- the **natively linked built-ins** (shell, settings, layout, toolbar and the like), and
- the **packaged** micros (the review micro among them, ADR 050's several micros per cartridge).

This deletes `INTERIM_GRANTS` and the interim install path. Built-ins stay native code in the GUI; only their identity
and grants live on the node. A built-in micro fetches its handles like any other (§3) and receives handles it uses through the
same egress, so the person sees one consent and audit view per cartridge (ADR 049 item 6) that includes the app's own
micros.

The interim intxt install (`claytxt: daytxt_native/interim.rs:173`, `install_intxt`, which calls the intxt install
through `operator_transport()`) becomes a normal `installCartridge` under the operator root capability. It is what the
root capability is for (ADR 045's privileged routes are the same shape). It stops being the GUI's boot step, since the
node already has the cartridge or `day` installs it.

**To confirm: the daytxt cartridge is the home of built-in micros.** The alternative is one cartridge per built-in, which
makes each independently disable-able and publishable (a toolbar cartridge, a settings cartridge). That is a good end
state, but a person would be asked about grants for each, so the proposal is to start with one cartridge and split it
when something needs to be disabled alone. Reasoning against it: the cartridge is what a person grants, and a person does
not grant "the toolbar".

## 7. Operator-written install config disappears

The review micro's `review.json` (`INTERIM_GRANTS`' `InterimConfig`: the drawer ids the operator writes, the GUI then
mints a document capability over) is an operator-written configuration the node should not need:
- The review micro reads its drawers through its **docs handle's drawer reach** (ADR 049 §6.2 and §2: a reach to granted
  drawers at the datum kinds in its `uses`), so no drawer id is written down.
- Its blobs are reached through `BlobsScope.drawers` (PR #244): the blobs pinned in a reached drawer's inventories.

Until both are used by the review micro, **the GUI's interim stays**: it keeps reading `review.json` and the micro keeps
its config datum. No part of this ADR's earlier steps waits on it; it is the last step of §11 for that reason.

## 8. One code path, and the node SDK

A GUI that boots the node itself (nothing holds it, so it becomes the holder) uses the same `micros` nodule through the
in-process transport. The GUI does not have a second way to mint, only a second way to reach the node.

**The SDK of D83 builds on claytxt's `day_node` crate.** The first draft proposed lifting `NodeClient` out of `day_cli`
into a new `big_node_client` crate. That is overtaken: `day_node` exists on `claytxt:` (added in commit `99e39289`;
`git show origin/stack/claytxt-55-ledger:src/day_node/lib.rs` and `holder.rs`) and already holds what that proposal would
have moved: the node directory (`NodeDir`) and `node.json` (`NodeSettings`), the boot lock (`NodeLock`, `LockRole`), booting
in process (`Booted`), and finding a running node's socket (`NodeDir::find_or_hold`, returning `Reached::Held` or
`Reached::Attached`). `day` and the GUI already share it. The `big_node_client` crate name is withdrawn, and so is the
proposal to lift everything out of `day_cli`.

- **What is left in `day_cli`** is the typed client: `NodeClient` with `call<M: Unary>`, `call_untyped`, `CallError`,
  `Mode`, the connect deadline and the in-process router with `Anonymous::Acts(operator)` and `Edge::Embedded`
  (`day_cli/client.rs` on this branch). Claytxt's copy of `client.rs` is still untyped (`call(nsid, verb, &Value)`), so the
  typed form is this lane's addition on top of `day_node`.
- **The typed client goes into `day_node` or beside it.** Proposed: **a `client` module in `day_node`**. The crate already
  depends on `big_node` and `reqwest` (`Attached` holds a `reqwest::Client`), so the cost a separate crate would avoid,
  client transport code pulled into a crate that is not the node, is already paid. A sibling crate is the alternative if
  an app shell that never reaches a running node (Android) must not link the client's method crates. **To confirm.**
- **Adds.** A typed micros client over it: `handles`, an `awaitHandles` follower that yields new handles on a channel
  (the host's loop; the long poll of §3 is the nodule's, and this follower is its client), and a micro egress that signs
  with the micro's actor over either transport (the in-process one replaces `claytxt: clay_node/egress.rs`).
- **What stays in `day_cli`.** The commands, argument parsing, output rendering and `config.rs`, and the `serve`/`node`
  commands that take the lock for `Serve`. The lock role is a parameter of `find_or_hold`, so a GUI is a `Command`-role
  holder, as `day` is.
- **Boot in process, then yield.** A GUI that booted the node is the holder, so `day` run next finds its socket and is its
  client. That is the existing `day`-to-`day` story with a GUI in one seat.
- **Merge hazard.** The nodules stack's layers #239 (the seed keyring) and #250 (CLI conventions) changed
  `day_cli/context.rs`, which claytxt moved into `day_node`. The file is not the same file on the two branches, so a
  textual merge will not flag it and a green rebase is not proof. Whoever lands second moves those hunks onto `day_node` by
  hand.

## 9. What stays in the GUI

- **Clay's collection store** (SQLite on native, IndexedDB on the web) and the collection contracts. They are Clay
  state, as ADR 049 §3 (asymmetry 4) and "Clay on the node" leave them: a collection another cartridge publishes is
  reached through a permbind, but the store itself is the host's.
- **The wasm components** the host runs (`wcmp`), loaded from the catalog of the cartridges the node holds. The micro's
  component bytes come from the node's artifact store; running them is the host's.
- **The micro config datum** is written by the host into Clay's store (§2 step 3).

## 10. Rejected alternative: a holder handoff

`day` yields the node to the GUI: the GUI asks the holder to release the lock, boots the node itself, and the CLI
becomes its client. Rejected. A yielding holder has live commands, peers and subscriptions to drain or move, and the
node's lock is also a storage lock (SQLite and the keyring). Nothing about the holder is cheap to hand off, and reaching
the running holder as a client makes it unnecessary: the GUI is a client of whoever holds the node.

The end state also falls out of this design: the node as its own long-lived service (`day node start`, a systemd unit),
with the GUI and the CLI both as clients. Nothing in this ADR requires it, and each step works with a holder that is
either one of them.

## 11. Order of work

Each layer is one small PR (see `.agents/skills/pr-stacks`). Lanes:

1. **Unprivileged `openActor`** (§2.2 item 1, node-78, #325, landed): a held identity capability opens children, and
   `attenuateCapability` introduces descendants. Nothing further to build. The node opens the `micros` actor at boot and
   hands it to the nodules service for the core cartridge (§2). Nodules.
2. **The core cartridge** (ADR 053): the preinstalled cartridge, its Lexicons as the source of `node_type_libraries()`,
   the runtime permbind of §2.2 item 2. It comes first among the cartridge work because it is where the micros nodule
   lives and holds the type library every other cartridge's admission needs. Nodules.
3. **The micros nodule, with `handles` and `awaitHandles`** (§2, §3, and §4's listener): the lowering moved out of
   `big_nodule` into a shared crate, the nodule in the core cartridge, e2e tests with a granted drawer and a re-mint.
   Needs 1 and 2. Nodules.
4. **The typed client in `day_node`** (§8), then the micros client on it. The typed client is independent of 1-3 and can
   land first; the micros client needs 3. Nodules, rebasing onto claytxt's `day_node`.
5. **The daytxt cartridge and deleting `INTERIM_GRANTS`** (§5, §6): nodules writes the cartridge's manifest and the
   install under the operator root capability; claytxt does the GUI side (the micros declared in the cartridge, the native
   built-ins' identities, removing the interim path).
6. **claytxt moves `clay_node`'s install side out.** `Minting`, `install_all`, `install_one`, `follow_grants`,
   `sync_cartridge_micros` and the `operator_transport` go; `clay_node` keeps the egress adapter and the GUI-side wiring
   on the shared client. Claytxt.
7. **`review.json` goes** (§7), once the review micro reads through its drawer reach and `BlobsScope.drawers`. Claytxt
   (the micro), with the nodules lane's reach and blobs scopes as they stand.

Steps 1, 2 and the typed client of 4 are independent and can land first; 3 needs 1 and 2; 5 needs 3; 6 needs 3 and 5.

## 12. To confirm

1. **The micros layer as a nodule in the core cartridge** (§2). The operator's direction of 2026-11-24 ("let us indeed try
   to make the micros layer itself a cartridge"); this ADR is the trial. What to watch is the lazy re-mint (§3); if its
   two edges prove unacceptable, a native follower is the fallback.
2. **Actor placement and descendant authority** (§2.2 item 1): settled. The micros actors live under a top-level `micros`
   subsystem actor, and `openActor` plus `attenuateCapability` need no further change. A privileged micros subsystem is
   rejected.
3. **The runtime permbind** to `listCartridgeGrants`/`awaitCartridgeGrants`, granted at install of the core cartridge by
   the node (§2.2 item 2, ADR 053 §3).
4. **The daytxt cartridge as the home of built-in micros** (§6), and not a cartridge per built-in.
5. **The typed client as a module of `day_node`**, versus a sibling crate (§8).
6. **The guest-egress listener** for a micro's calls over the socket, versus a proxy method (§4).
7. **Returning the micro's `actor` capability from `handles`** (§3): the host process signs for the micro. The signing only matters when
   `day node start` holds the node in another process and the GUI reaches it over the socket; in process it is the same as
   today. It follows from holder binding as built; the alternative is a node-side signer, which makes every micro call a hop through the node.
