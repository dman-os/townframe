# ADR 052: The node hosts micros: a `micros` service, attach, and one node client

**Status:** Draft (2026-11-24). Proposed by the nodules lane and agreed in shape with the claytxt lane; the operator signs
off. Nothing here is built. Decisions marked **To confirm** need the operator.

**Amends:** ADR 049 §6.1 (the `micros` service actor is a real service on the node, no longer a composition the GUI
opens; the "same holds for the `micros` service actor" line becomes this ADR) and ADR 050 (§3's micro surface gains its
node-side host; the micro manifest is unchanged).

Related: ADR 044 (cartridge configuration documents), ADR 045 (method routes; the operator route is the precedent for a
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
only one of the two runtimes has such a service. This ADR gives the other one.

## 2. A `micros` service, isomorphic with `nodules`

The node gains a `micros` service next to `nodules`. Like it, the service actor is opened at boot beside the others
(`big_node/lib.rs:372`, where `nodules` is opened) and holds, durably, every right it hands down (ADR 049 §6.1). For each
enabled micro of each installed cartridge, the service:

1. **Creates the micro's actor** under the `micros` service actor, at `micros/<cartridge>/<micro>`, with `USE` rights, as
   `open_actor` does for a cartridge (`big_nodule/state.rs:3104`). The path is the micro's id from ADR 050 §1
   (`<cartridge id>/micro/<name>`), so an actor is durable and survives the GUI that asks for it. Today the GUI opens it
   per boot with root authority and `Rights::all()` on the subsystem actor.
2. **Mints its handles** with that actor as `subject`, from the micro's `uses` intersected with the cartridge's grants
   (ADR 049 §3, §4.1): dispatch (the operations it binds), docs (`#datum` kinds in each granted drawer), blobs
   (`BlobsScope`, including `drawers`), and drawers when the `uses` reach them. The result has the shape of
   `RoutineCapabilities` (`big_nodule/executor.rs:41`: `dispatch`, `docs`, `drawers`, `blobs`), one per micro instead of
   one per routine. The lowering from `uses` to scopes is the same code as for routines; it must not be copied.
3. **Composes the micro's config datum**, the record the host puts in the micro's configuration slot. It carries what the
   node knows (today: the document capability under `docCap`, which the GUI splices in
   `claytxt: clay_node/lib.rs` `install_one`). The node does not write it anywhere: the collection store is Clay's (§9),
   so the host writes the datum the service returned.
4. **Re-mints when grants move.** It follows the same feed `awaitCartridgeGrants` is built on
   (`GrantsFeed`, `big_nodule/requests.rs:623`, published at the end of every reconcile) and re-mints in 6.4's order: the
   micro is registered not ready, the old handles are retracted, new ones minted (`big_nodule/state.rs:1028`,
   `apply_nodule_capabilities`). It then bumps the micro's handle revision (§3).
5. **Mirrors drawers.** A drawer granted to a micro must also be remembered in the known-drawers registry or the micro's
   reads stay empty (`claytxt: clay_node/lib.rs` `mirror_drawers`). That step moves into the service unchanged.

**Every grant sits on an explicit grant to the service actor.** ADR 049 §6.1's rule applies as written: the `micros`
service holds, as a delegator, the drawers and rights it hands down, granted when the person's grant is made, and the
hold is durable. Nothing is minted on the strength of an operator being present per call. The operator root capability
is what the GUI uses to ask (§3), not what mints.

A cartridge's `micros` and `nodules` stay parallel subsystems with no cartridge-level parent (ADR 049 §8). Disabling a
cartridge retracts both runtimes' handles.

## 3. Methods

All three are in the `org.example.daybook.micros.` namespace and live beside the cartridge methods that follow the same
conventions (`big_cartridges_core/protocol/requests.rs:254`).

```text
micros.attach { cartridgeId, micro? }
  -> { micros: [{ micro, actor, handles: { dispatch, docs, blobs }, config }], revision }

micros.awaitHandles { since: { <cartridgeId>: revision, ... }, holdMs }
  -> { cartridges: { <cartridgeId>: { micros: [...], revision } } }      // only those that moved
```

- **`attach` returns all of a cartridge's micros in one call** (or one, when `micro` is given), so booting the GUI is one
  round trip however many micros the cartridge ships. It is idempotent: attaching again returns the live handles, it
  does not re-mint. It answers at once and never holds.
- **`awaitHandles` is `awaitCartridgeGrants`' pattern.** A revision is opaque and names the boot it was taken in. A
  `since` that is missing or not current answers at once. Otherwise the call holds up to `holdMs`, clamped to
  `AWAIT_MAX_HOLD_MS` (15 s), until a watched cartridge's handles change; an expired hold answers unchanged. The keys of
  `since` are the cartridges the host is watching, so a host with several cartridges polls once. Like its model it is
  `long_poll`, and a closed feed (the service stopping) ends the hold as an expiry does
  (`big_nodule/lib.rs:469`).
- **Nothing blocks on the long poll.** The host mounts each micro with the handles `attach` returned and re-renders the
  mounts of a cartridge when `awaitHandles` reports a new revision. A mount never waits for the poll, and a micro that
  loses a handle sees a retryable not-ready answer between retraction and re-mint (ADR 049 §6.4), never a stale handle.
- **`actor`.** The result names the micro's actor capability too, because dispatch requires it: dispatch admits a call only when the capability's holder is the signer's actor (ADR 037; `big_nodule/egress.rs` states the check dispatch runs), and the GUI is a separate process from the node, so something outside the node has
  to sign as the micro. That is the signer the in-process composition holds today (`claytxt: clay_node/egress.rs`,
  `MicroEgress::install`). It carries `USE` only, and it is the host's, never the guest's: the guest names an operation,
  the host attaches handle and signer (ADR 049 §3, asymmetry 2). The actor capability stays in the host and never crosses into the guest: the host overwrites whatever `authorization` a guest request carries (`claytxt: clay_node/egress.rs:12`).

**What is privileged, and how the GUI is authorized.** Like `listCartridgeGrants` and `awaitCartridgeGrants`, all three
are `privileged`: they hand out capabilities, so only the operator may ask. The service recognises the operator by the
resolved root capability (`ResolvedAuthority::is_operator_root`, `big_cabinet_core/authority.rs:302`, as
`big_nodule/lib.rs:300` does) and nothing it was configured with. Over the holder's Unix socket the kernel's peer
credentials make the connecting process the operator (`big_dispatch/xrpc.rs:44-50`, `Anonymous::Acts`); in process, the
GUI's client acts as the root capability. The GUI is therefore a client with operator authority that mints nothing: the
handles it receives are scoped to each micro and bound to its actor, so a compromised micro still holds only what its
`uses` and the grants give.

## 4. Egress

A micro's `dispatch.local` request is an XRPC call to the holder's dispatch carrying the micro's minted dispatch
capability as the bearer and its actor as the signer. The shared client in §8 is that XRPC client, and the socket
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

- **The host's datum vocabulary ships as installed cartridges, not as `schemas`.** Two cartridges: a **core cartridge**
  for the node's built-in type library (`org.example.daybook.defs`, hardcoded today as `node_type_libraries()`; the
  ledger's "core cartridge" to-do), and the **daytxt cartridge** of §6. The node then admits the same Lexicons wherever a
  client reaches it. `day` already boots with an empty `SchemaRepo` and no user datums without a cartridge (ledger), so
  this also fixes `day`. Open: boot order, the core cartridge before every other; the nodules lane settles it with the
  core-cartridge work and reports.
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
and grants live on the node. A built-in micro attaches like any other (§3) and receives handles it uses through the
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

## 8. One code path, and the node client crate

A GUI that boots the node itself (nothing holds it, so it becomes the holder) uses the same `micros` service through the
in-process transport. The GUI does not have a second way to mint, only a second way to reach the node. This is the
`NodeClient` shape (`day_cli/client.rs:162`, `connect`): find the holder's socket, else boot the node in process and
serve the socket for others, then make typed calls (`call<M: Unary>`, `:259`).

**The node client crate is the SDK of D83.** It is `NodeClient` lifted out of `day_cli`, and the GUI, `day` and
standalone product binaries (`day-intxt`, D84) share it.
- **Scope.** `NodeDir` (locating the node, probing its socket, the lock), `Booted` (boot on the lock, with the
  socket and the in-process transport), `NodeClient::{connect, call, call_untyped}`, `CallError`, `Mode` and the
  connect deadline. The in-process router with `Anonymous::Acts(operator)` and `Edge::Embedded` is part of it.
- **Adds.** A typed micros client: `attach`, an `awaitHandles` follower that yields new handles on a channel, and a micro
  egress that signs with the micro's actor over either transport (the in-process one replaces
  `claytxt: clay_node/egress.rs`).
- **What stays in `day_cli`.** The commands, argument parsing, output rendering and `config.rs`, and the `serve`/`node`
  commands that take the lock for `Serve`. The crate takes the lock role as a parameter so a GUI is a `Command`-role
  holder, as `day` is.
- **Boot in process, then yield.** A GUI that booted the node is the holder, so `day` run next finds its socket and is its
  client. That is the existing `day`-to-`day` story with a GUI in one seat.

**To confirm: a new crate or an existing one.** The proposal is a new crate (`big_node_client`, in the `big_` backend
family; the operator names it, ledger "Naming roles") with `Booted` and `NodeDir` moving into it from `day_cli`.
Extending `big_node` would put the client's transport code (the socket HTTP client) into the node itself, which only
clients need. Reasoning against a new crate: another crate boundary and public-API surface to carry
(`cargo x guardrails api`), when `day_cli` could be split into `day_cli` as the lib and `day` as the bin.

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
node's lock is also a storage lock (SQLite and the keyring). Nothing about the holder is cheap to hand off, and attach
makes it unnecessary: the GUI is a client of whoever holds the node.

The end state also falls out of this design: the node as its own long-lived service (`day node start`, a systemd unit),
with the GUI and the CLI both as clients. Nothing in this ADR requires it, and each step works with a holder that is
either one of them.

## 11. Order of work

Each layer is one small PR (see `.agents/skills/pr-stacks`). Lanes:

1. **The node client crate** (§8): extract `NodeClient`, `NodeDir` and `Booted` from `day_cli`; `day` depends on it.
   Nodules.
2. **The `micros` service, with `attach` and `awaitHandles`** (§2, §3, and §4's listener): the actor opening and handle
   minting reusing the lowering of `nodules`, the feed, e2e tests with a granted drawer and a re-mint. Nodules.
3. **The daytxt cartridge and deleting `INTERIM_GRANTS`** (§5, §6): nodules writes the cartridge's manifest and the node
   side (the core cartridge, the install under the operator root capability); claytxt does the GUI side (the micros
   declared in the cartridge, the native built-ins' identities, removing the interim path).
4. **claytxt moves `clay_node`'s install side out.** `Minting`, `install_all`, `install_one`, `follow_grants`,
   `sync_cartridge_micros` and the `operator_transport` go; `clay_node` keeps the egress adapter and the GUI-side
   wiring on the shared client. Claytxt.
5. **`review.json` goes** (§7), once the review micro reads through its drawer reach and `BlobsScope.drawers`. Claytxt
   (the micro), with the nodules lane's reach and blobs scopes as they stand.

Steps 1 and 2 do not depend on a claytxt change, so they can land first; step 3 needs 2; step 4 needs 2 and 3.

## 12. To confirm

1. **The daytxt cartridge as the home of built-in micros** (§6), and not a cartridge per built-in.
2. **A new node client crate** and its name, or extending an existing crate (§8).
3. **The guest-egress listener** for a micro's calls over the socket, versus a proxy method (§4).
4. **Returning the micro's `actor` capability from `attach`** (§3): the host process signs for the micro. It follows from
   holder binding as built; the alternative is a node-side signer, which makes every micro call a hop through the node.
