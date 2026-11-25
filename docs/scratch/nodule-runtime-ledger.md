# Nodule runtime and dispatch ledger

Status: Active
Owner: Townframe maintainers
Updated: 2026-11-24
Scope: The nodules lane's decision ledger: the standing decisions made with the operator, the current state of the stack, the to-do list, what is deferred and what awaits the operator. Read this first after any context compaction, together with AGENTS.md, `.agents/skills/pr-stacks/SKILL.md` and the ADRs it points at. History, measurements and per-slice landing notes are in `nodule-runtime-ledger-archive.md`.

## How to use this ledger

- Append decisions with the date. Never silently rewrite one; mark it superseded and say by what.
- A decision that an ADR now states is condensed here to its operative rule plus the ADR pointer. The ADR is normative; this ledger is the working log.
- Keep **Current state** and **To do** current whenever work starts or lands. Delete finished to-dos from the list (the git log is the record); put long-form reasoning in the archive, not here.
- Trimming: move text verbatim into the archive under a dated heading and leave a one-line pointer in the **Archive index**. Nothing is deleted.
- Don't re-ask settled questions. Check the archive's "Open questions" block before asking the operator something.
- The ledger is a stack-layer doc: a small update that records one layer's work rides in that layer; a substantial batch of decisions is its own docs layer (pr-stacks).

## Standing decisions

D-numbers are stable names; the archive holds each one's original text.

### Dispatch and the runtime (ADR 027, 045, 046)

- **D1/D2. The runtime models services, facilities and lifecycle; everything else is a binding.** Dispatch is the runtime's XRPC reverse proxy in `big_dispatch` (D17): stateless per call, ocap admission, NSID to provider resolution, readiness gating, then forward. Routing table modelled on wash `Router`: a sync snapshot lookup with a reverse index per provider, driven by the reconciler, unbind idempotent. Providers: `BuiltIn` (typed client) and `Wasm`. Settle runtime and dispatch design questions by gRPC/Envoy/wash/XRPC precedent and name it.
- **D2c. Router states.** A not-ready provider is uncallable by construction (`ProviderState::{Ready, NotReady}`). A disabled nodule is unbound, not registered NotReady. `Denied` is decided before lookup so callers cannot probe which methods exist. Authority prefixes match whole NSID segments.
- **D2/D2e/D39. One instance per dispatch; authority is a header.** A fresh wash instance per call (`pool_size: 0`). Nodule-to-nodule calls egress to the reserved authority `dispatch.local`; the caller identity is the workload's actor (from the unforgeable workload id), the guest supplies only a capability id, dispatch checks the cap's holder is that actor. No other egress except `#net` (D52-D56, ADR 049). The header contract on every hop: `Authorization: Bearer <token>` (bare capability id to and from a guest), `x-nodule-capability` (the nodule's own grant), relative `x-timeout-ms` re-derived per hop, `x-request-id` per hop, W3C `traceparent` for the chain, `idempotency-key`. An exhausted budget has one error, `ContextError::DeadlineExceeded`; an already elapsed deadline fails fast (gRPC).
- **D22. HTTP-shaped calls.** The caller's credential is a header and request context, never a body field. UDS peer credentials are an internal request extension, never a header a client could forge; reverse-proxy rule if a hop ever needs headers (strip client copies, then set). Peer credentials stay behind `cfg(unix)`; Windows later over TCP with a token. Resource capabilities that designate what is acted on (`capability: CapId` on doc, drawer and blob data operations) stay in the body.
- **D6. Physical rules.** No I/O on admission. Validate against the Lexicon at the edges only. No durable write per non-durable call. Keep the provider behind a service boundary so the Wasm host can move out of process.
- **D41. Method Lexicons (atproto precedent).** Every `Method` carries an atproto Lexicon emitted from its request/output/error types (`method_lexicon`); dispatch validates input before and output after `Provider::call` (400 `InvalidRequest`, 500 `InternalServerError`); GET query strings are coerced by the Lexicon's `parameters`. Errors: XRPC `error` is the outer variant name, `message` its Display. Bodies are objects (lists are wrapped), query requests are flat primitives.
- **D31. Transports.** Remote clients use HTTP and WebSocket over iroh; no TCP listener for clients, no TLS on TCP. GET/query through `/xrpc` is plain to-do work (done in D41 step 4).
- **D45 / ADR 045.** Method routes (`dispatch.route/<pattern>` datums, longest match wins, privileged `nodules.setMethodRoute`); slot takeover is an explicit dev hook (`node.json` `dev.allow_slot_takeover`, `day carts dev-takeover`), not a default. Methods declare `privileged`; a scope carries a monotone `privileged` bit and the `*` authority.
- **D46 / ADR 046.** Services other than the cabinet program on the docs service, not `big_repo`. The work list is four cabinet services: (A) membership/access watch plus list and get, (B) doc materialization watch, (C) doc interest/readiness lease, (D) the node's own peer id. Sync and transport plumbing stays below the cabinet.

### Cartridges: identity, manifest, config, init (ADR 028, 044, 047, 049, 050)

- **D8/D34. Plug is renamed cartridge ("cart").** One package format for every kind; micros and nodules are the classes of thing it contains. Config is node-local: syncing it is backup and never activates anything on another node. Manifest requests, configuration grants, capabilities are minted from grants: one auditable story, answerable statically.
- **D10-D13 / ADR 028.** The registry is a catalog (a grouped drawer), not an enabler; runtimes are its node_modules and own enablement, downgrades, grants and readiness. Id is `<DID>/<name>`; every fork is a new id; a substituting fork is an explicit node-local act; upgrades accept only monotone versions from the same id and never widen grants silently. Dependencies are by NSID (a breaking change is a new NSID; `minRevision` for growth); plugs vendor the Lexicons they depend on; the lockfile records resolution at enable time. Retention is by reference (the enabling runtime's own drawer pins the blobs), not leases.
- **D7 / ADR 047 §8.** Initializers: `initialization` is an ordered list, each entry with its own `key` and mode (`Once` per node per revision, or `EveryStart`); done-ness is host-local SQLite keyed (cartridge, key, revision); idempotency key `nodule/<cartridge>/<revision>/<key>`; the nodule is `NotReady(Initializing)` until its inits pass, `InitFailed` is retried on the next reconcile trigger. An initializer targets only an operation its own cartridge implements (foreign targets dropped 2026-11-17). Init calls run off the state actor and stale results are dropped by epoch.
- **D40/D57/D58 / ADR 050.** Cartridge = shared infrastructure (identity, Lexicons, components, permbinds, settings, grants) plus an open `$type` list of surfaces (nodule, micros, later editor, tiles); several micros are allowed. A micro's `requires` is replaced by permbinds plus `uses`; `publications` becomes `endpoints`; contributions are written by a micro's `initialization` or declared statically (both first-class); `#method` is routed by NSID, `#callback` is reachable only through a held link; built-in micros get synthesized host cartridges under the local DID. Contributions carry no authority yet: any micro may write any row for now, with per-contributor scopes kept as bookkeeping. Crates follow `<product>_core` / `_nodule` / `_micro`.
- **D42/D54 / ADR 044.** One config document per cartridge in the shared config drawer (cartridge-declared settings datums, one datum per grant, so each grant has its own signature and author) plus one per-node document for installs, pins and enablement. `install` holds a `DatumPin`. `resolveCartridgePin` resolves a pin as the revision it selects (the registry reads the envelope at the pin's heads and checks it as a publish does); `awaitCartridgeGrants` long-polls a cartridge's grants (operator only; revision is `<boot uuid>.<feed seq>`).
- **D52-D56 / ADR 049, FDR 009.** One permission model: a doc scope reaches drawers with separate read and write datum kinds; a doc named directly in a scope keeps its own kinds; `branch` needs the whole source, `merge` is checked per datum changed against the actor's write kinds; a drawer reach reads the drawer's own document (D55). Grants are per-cartridge datums in the person's nouns (drawer, cartridge, net, method, authority). A runtime grants itself first, then its children: a created drawer is owned by the `nodules` service actor, which grants the cartridge actor; a grant made from a capability lives only in memory, so the service's hold on a granted drawer is durable. A nodule may create its own drawers by default (XDG-style; check with the automata drawer rework before building). Missing authority is the cartridge's `MissingAuthority`, carried across cartridges; drawer and docs calls keep `Denied`.
- **Grants survive uninstall (2026-11-19).** Uninstall removes the install (pin, artifact references, dev takeover) and nothing else; settings and grants stay in the cartridge's config document and the service's durable drawer hold is released only when no accepted grant names the drawer. Removing them is explicit (`setCartridgeGrant` revoke, `clearCartridgeConfig`).
- **`#net` (D53/D56 / ADR 049 §3).** Deno `--allow-net` syntax: `host` any port, `host:port` one, no scheme, IPv6 as `[::1]:8000`. Loopback, link-local, the metadata address and private ranges are denied by default, and every address DNS returns is re-checked; the machine's loopback is reachable only through the sentinel host, gated by a per-cartridge port allowlist and a node switch. Redirects are checked per hop. `Cx::fetch` streams. Grant `grant/net/<host[:port]>`.
- **D47/D49.** Week focus was the permission model as one story and micro/nodule isomorphism. Sandbox and volume are Docker-shaped `localState` kinds (ADR 047 amendments b-d, proposed not accepted; ADR 051 carries the guest filesystem now).
- **Vocabulary (D56/D57/operator).** UI says cartridge; relay over backup; micro/nodule/host-boundary vocabulary is this lane's to lead, and claytxt is told before anything they use changes. Prefer WASI interfaces over bespoke host APIs; micro-to-micro stays host-mediated; WIT `implements` (labelled exports) is not usable for Rust guests yet (wit-bindgen #1551), so routines keep one `wasi:http/handler` export with routing inside. Shared code between `clay_mdk` and `big_ndk` goes in the shared crate; nodule-only things stay in the NDK.
- **D50.** Sidecars are stored as JSON text checked by their own `contentSchema` (ADR 021 §3). "Any JSON" datums are an existing Lexicon pattern: a record whose `value` is `unknown`. clay_collection's leased partial replication stays required (each micro has its own address space). This lane owns `clay_docs` (typed docs layer, the node-fed mirror, query mounts).
- **D51. Reviewable wire surfaces.** Every published Lexicon and sidecar is a checked-in snapshot with an evolution check (items 1-2, #170, plus per-item `since`/`deprecated`/`unstable`, unstable by default pre-release, stabilizing needs the operator's sign-off, ADR 021). Still to come: the manifest-contents snapshot (waits for real built-in cartridges) and the datum slot registry.

### Authority, identity, the operator (ADR 037, 043)

- **D4.** Authority is ocap throughout; do not hard-code drawer-only grants. Readiness is per service/provider.
- **D24/D28.** Installing a cartridge uses the caller's capability, not a standing service grant, and takes a cartridge in whatever form it arrives. Blob retention targets only inventories the node can write; a write keyhive refuses becomes a typed error mapped to `Denied`.
- **The operator is superuser only through its root capability (2026-11-19, #245 node-68).** The operator identity check was removed: there is no operator in service configs, and the operator converts its standing into whatever grant it wants through the root capability. This supersedes the old wording that a same-uid UDS peer "is the operator".
- **D43 / ADR 043.** The CLI is nobody by default; operator access needs a protected secret (keyring). Clients pair, then hold a persistent identity as a subsystem actor of their app; the credential is identity not authority; authority comes only from grants. The pairing ADR is sequenced with ADR 037 (b), channel identity. "Pairing" may be renamed "enrollment".
- **D33/D56.** Every keyring secret is scoped by node id; the node's identity seed lives in the keyring (via `secrets_rs`; a systemd-credentials backend exists, the kernel-keyring fallback warns), `node.json` keeps the public node id. Sockets are `$XDG_RUNTIME_DIR/day/<node id>.sock`.
- **D53.** iroh public relays (n0) stay off by default; an opt-in `node.json` setting may name our own relays.

### Data model, storage, perf rules

- **D27/ADR 020.** The datum gate verifies each signature at its introducing change and that the value at the combined signature heads equals the value now; hidden losing-conflict operations are not checked and there is no erasure guard (anyone with write access can break a doc; the gate defends against forgery). Reference defs are Lexicon defs (`org.example.daybook.defs`, `#datumPin` etc., ADR 035); dtxt: URLs are node-side (this lane owns everything below intxt).
- **D32/D33.** Datums follow Lexicon and Automerge scalars: no integer width formats (bounds only; `minimum >= 0` stores `Uint`), floats are one f64 decimal string, `F64` not `Float64`, no tuples or 128-bit/pointer-sized leaves.
- **D25.** Scale is a benchmark: set targets from napkin costs, measure in release, measure memory. Targets: note create about 5-20 ms and flat with drawer size; 10k notes replicate in tens of seconds to minutes. `big_node::e2e::costs` holds the harness; the growth pin must not be loosened.
- **D29/D30/D36. big_repo is a rewrite candidate** (after the upstream keyhive/subduction rewrite). Until then fix obvious costs in place and flag every wrong turn to the operator; fork fixes go straight into our keyhive/subduction forks (`townframe-changes`), ported at migration. No caches or LRUs before the low-hanging costs are fixed. Flagged and unfixed wrong turns are listed in the archive (D30 block).
- **D37.** Many nodes in one SQLite database is not a goal. Test threads under load and ledger housekeeping are the manager's call.
- **Command ids** are per-node retry protection (7-day ledger, `DONE_TTL_MICROS`), not cross-node identity; documented on `CommandId` and the create methods.
- **Concurrent document creation** (the intxt meta-doc race) is the cabinet's weak spot: get-or-create on the synced roster was rejected by the operator (fails with writers offline). Interim rule: `CreateMetaDoc` must never silently repoint an occupied `(root, kind, name)` slot; keep both or refuse with a typed error. Needs a general design conversation; coordinate with the automata drawer rework.

### Node shape and CLI (ADR 033, 038, 042)

- **D14-D20 / ADR 033.** A node owns its data and is its sync identity; `big_node` knows no users; services, clients and attachments; three access paths (in-process, UDS with a token handshake, network with a capability per call); nothing assumes one node per process; networking and peers belong to `big_node` with one key for keyhive and iroh and an embedder-supplied peer policy.
- **D21/D48 / ADR 038.** Sisters and mothers vocabulary is unsettled: first enumerate the P2P forcing functions (`docs/scratch/p2p-requirements.md`), then name the relationships. A plain node syncs only drawers it remembers with `mirror: true`; a relay mirrors every drawer it is made a member of. Inbound admission (F11): a non-relay node accepts inbound only from sisters.
- **D42 CLI.** `day node init|start|stop|status`, `day carts`, `drawers` (keyhive verbs: `add-member`, `revoke-member`, `remember`, `forget`), `exec`/`x`, `grants`, `dump`, `completions`; plural namespaces accept the singular. `day` never builds source. Whoever opens the node serves its socket under the lock (holder-serves, #100).
- **Client code vs methods (D42).** Default to client code writing with its own capability; a method is for durable or multi-step work and as an authority boundary for weakly privileged callers.
- **D35/ADR 042.** User-facing a cartridge offers commands; developer-facing they are XRPC methods. Local-only methods (`blobs.importFile`) are refused unless the call origin is local (ADR 042 §3).
- **D38.** `big` is the backend framework (nodule kit `big_ndk`), daytxt the app, dayframe the user's computer, webnode the browser deployable; where frame ends and node begins is undecided.
- **D48. PDS.** Starts as a separate service (off the shelf: rsky-pds, atproto-crates); every node is a PDS is rejected; the border between daytxt and the atmosphere is the question (draft ADR 048).

### CLI conventions (2026-11-19, D57 follow-up; landed #250 node-73)

- One fallible writer `Out` (`src/day_cli/out.rs`); a closed stdout exits 0. `--json` on every data command.
- Tracing at info only for a foreground `day node start`, otherwise warn unless RUST_LOG is set. INSTR is off by default (`instr=info` opts in).
- Exit codes are in the top-level `--help`. Colour only on a TTY, honouring NO_COLOR.
- `cargo x cli docs|man` generates `docs/cli/day.md` and the man pages; shadow-rs supplies `--version`.
- Miette is deferred until commands edit or consume text.

### Verbs (2026-11-19; #246 node-69)

- `day node start` runs in the foreground, `--bg` backgrounds it, there is no `run`, and the daemon module became `serve`. User-visible wording says "server".
- `day dump <what>` is a raw debug view of the node's drawers, docs, carts and grants (#247 node-70); it is a debug UI, not polished on purpose.
- Typed node calls: `NodeClient::call<M: Unary>`; commands no longer hand-build JSON.

### Guest VFS, ADR 051 (draft, #251; 2026-11-19)

- The guest filesystem is the working mount plus `materialize`. Every local state comes with locks (there is no single-writer rule). Visibility is private by default.
- Public permbinds are inheritable ("option 2", operator 2026-11-19).
- A virtual `wasi:filesystem` is deferred.

### Smaller 2026-11-19 decisions

- **Lexicon maps render as `unknown`**, consistent with the other scopes.
- **Sharp-edges (#249 node-72).** `DocsError::DocNotFound` and `UnknownHeads` carry `doc_id`. Open, recommended: ids on `UnrelatedHistory` and `Conflict` (yes); keep the `CommandId` name (yes); location and backtrace only under RUST_BACKTRACE.
- **`getNodeInfo`** should also report drawer ids (present, originated, config) so `day dump node` can walk them (To do).

### Operator policy (2026-11-24)

- PRs open as soon as possible. Push everything from the laptop and mothership, WIP included. Mothership is being retired for an x86 device. Each stack layer is the lane's own place to push freely.
- The lesson: 13 layers sat unpushed from 2026-11-19 to 2026-11-24.

### Operator decisions (2026-11-24, micros and the node SDK)

- **The micros layer is a cartridge (ADR 052 rewrite).** "Let us indeed try to make the micros layer itself a cartridge." The `micros` service is a nodule in the core cartridge, not native node code. It opens `micros/<cartridge>/<micro>` actors under its own actor (one segment per open) and mints narrowed dispatch, docs and blobs handles from handles it holds: ADR 049 §6.1's "a runtime grants itself first, then its children". What stays native: the authority, dispatch, the cabinet and the `nodules` host. Kept from the claytxt agreement: the `attach` batch form, `awaitHandles` over a set, mounts that never block, the actor capability staying in the host, the separate guest-egress listener stamping `Origin::Remote`, the daytxt cartridge for built-in micros, `review.json` going away. Re-mint is lazy: `awaitHandles` long-polls the grant feed and re-mints itself (no background follower); a native follower is the fallback if the trial fails. ADR 052 is the amended text; it carries the To-confirm list.
- **The core cartridge (ADR 053, new, draft).** The node's built-in surfaces ship as one preinstalled cartridge pinned to the node build, which cannot be uninstalled or disabled; its Lexicons become the source `node_type_libraries()` reads (admission, dispatch and `big_nodule_core` validation, the schema compiler, the wire-surface snapshot). Bootstrap rule proposed: admission reads the copy embedded in the binary, never the store; the store's record is for audit and is rewritten when it differs. Upgrades follow the monotone rule; a binary older than the install refuses to boot. Catalogue of what else moves (host-internal datum tags, built-in methods' Lexicons) is in the ADR. Supersedes the "core cartridge" to-do below. Open: who signs the preinstall, embedding vs fetching the micros component.
- **Descendant-scoped authority (node-78).** `openActor` and `mintCapability` stop being `privileged` when the new actor or the `subject` is strictly under the caller's own actor (`attenuateCapability` and `retractCapability` are already unprivileged). Plus an explicit runtime permbind, granted once at install of the core cartridge, that lets the micros nodule read other cartridges' installs and grants (`listCartridgeGrants`/`awaitCartridgeGrants`). Open: pinning `mintCapability`'s free-form `object` to a held handle.
- **`getNodeInfo` reports drawer ids (node-79).** Present, originated and config drawer ids, so `day dump node` can walk them (this is the existing to-do, now a layer).
- **ADR 053 accepted (2026-11-24, relayed by claytxt).** The micros component is embedded in the node binary (the GUI must work standalone, no network at startup). The core cartridge's id is `<node publisher DID>/core`, signed by the node's own key (`big_cartridges` publish signs unsigned envelopes in the node's namespace), so no build-time key exists and the id differs per node; nothing references it by id. A downgrade falls out of the registry's `Downgrade` rule.
- **Micros actors (2026-11-24, operator).** One subsystem actor per runtime: `micros` → `micros/<cartridge>` → `micros/<cartridge>/<micro>`, a sibling of `nodules/<cartridge>`. The node opens `micros` at boot and the core cartridge's nodule acts as it, so its micros are its own subtree and node-78's `openActor`/`attenuateCapability { subject }` reach them with no new privilege (holder binding is why it must be the nodule's own actor, not a handle it holds). A privileged micros subsystem was the rejected alternative. ADR 052 is reframed: the node manages micro installs and authority, Clay runs the micros; `micros.attach` is renamed `micros.handles` (node-82).
- **D83. No CLI-extension surface.** The node is a shared service plus an SDK; products (`day-intxt` and the like) are clients of the node, not extensions of `day`.
- **D84. Git-style plugins.** Standalone product binaries are `day-<name>` executables found on PATH; built-in subcommands win over a same-named plugin.
- **The SDK builds on `day_node`.** claytxt's `day_node` crate (`origin/stack/claytxt-55-ledger`, commit `99e39289`) already holds the node dir, `node.json`, the boot lock, booting in process and finding a running node's socket (`Reached::Attached`), shared by `day` and the GUI. What is left in `day_cli` is the typed client (`NodeClient`, `call<M: Unary>`); it goes into or beside `day_node` (proposed: a `client` module). The `big_node_client` crate proposal and lifting everything out of `day_cli` are withdrawn. Merge hazard: #239 (seed keyring) and #250 (CLI conventions) changed `day_cli/context.rs`, which claytxt moved into `day_node`; the second to land moves those hunks by hand.
- **The GUI attach plan.** The GUI is a client of whoever holds the node: `find_or_hold` attaches to a running `day` node's socket (operator authority over the Unix socket's peer credentials), or, with no holder, boots in process and serves the socket so `day` run next attaches to the GUI. It calls `micros.handles` and `awaitHandles` and signs for each micro with the `actor` capability the host keeps; micro calls go over the separate guest-egress listener. A holder handoff was rejected (ADR 052 §10). Order of work is ADR 052 §11.

### Shelved to this week (D50/D56)

- The atmosphere border (D48, ADR 048 draft, the spaces experiment) and dpaths (FDR 001). The operator will talk to the other lanes before starting.

## Current state (2026-11-24)

- **On main** (`aa0fcdcc`): #93-#100 were squash-merged on 2026-11-19: ADRs 042-046 and this ledger (#93), `listDocDrawers`/packed signatures/size limits (#94), blobs over the wire (#95), capability binding and typed drawer errors (#96), method grants and no privileged ceiling (#97), ADR 045 routes and dev takeover (#98), `day node` replaces daemon/init/status (#99), holder serves (#100). Before them: #79-#84 (clay, dayframe, big_node, datum_core).
- **Open node stack** (all draft PRs, base chain linear): #105...#199 (node-09 ... node-55), then #233...#251 (node-56 ... node-74), and #252 (node-76, cleanup). The operator has reviewed through #114 (node-18). This ledger trim is the node-75 layer (`stack/node-75-ledger`).
  - node-09 .. node-55 carry (git log of the tip): ADR 047 manifest and cartridge config docs (044), byte bodies (042), routine capabilities, the ADR 049 permission model and its implementation (grants, `#net`, reach kinds), the wire-surface snapshots (#170), ADR 050.
  - node-56 .. node-74 carry: the durable service hold and the keyring identity seed, INSTR off by default, the operator as root capability only (#245 node-68), blobs scopes reaching drawers (#244 node-67), typed clients, the daemon rename and node start (#246 node-69), dump (#247 node-70), the `big_ndk_testing` operator caller (#248 node-71), sharp edges (#249 node-72), the CLI story (#250 node-73), the ADR 051 draft (#251 node-74).
- **Blob scope and drawer reach (#244 node-67):** `BlobsScope.drawers` reaches drawers monotonically under covers; `blobs.read` and `stat` admit a digest pinned in an inventory of a reached drawer. A docs scope's drawer reach reads the drawer's own document (D55).
- **CI:** draft PRs skip checks; only the tip is greened. Full suites run on GitHub CI; local runs are narrow.

## To do

Items are open unless noted; verify against the stack before starting.

- Done 2026-11-24: `getNodeInfo` drawer ids and `day dump node` (node-79, #326); `openActor` unprivileged and `attenuateCapability { subject }` (node-78, #325); `node.json` `listen.tcp` (node-80, intxt C15).
- intxt citizenship C16-C18 (2026-11-24, from the web viewer #310 reading a node over TCP XRPC):
  - C16: there is no operator path to mint a dispatch capability (`dispatch.mintCapability` is not on XRPC), so the viewer carries three shared-secret handles per node.
  - C17: anonymous XRPC reads (readDatums/pollDatums/blobs.read) of drawers granted Read to keyhive's Public agent, with no bearer. Folded into C11.
  - C18: handles die at restart. This is by design (ADR 037 §4-5, no sturdyrefs). A viewer holding bearer handles across restarts is the sturdyref case ADR 037 says needs an operator decision. The leaning is C17 for public drawers, or an admitted viewer actor with a durable grant that authenticates its channel and gets handles minted on connect. This is an operator question; see below.
  - A 403 (not 404) for an unknown drawer under a handle that does not reach it is intended: a 404 would be an existence oracle.
- The typed client in `day_node`, the micros nodule (ADR 052 §11).
- Follow-ups queued in #252 from the #106-#114 review threads (answered 2026-11-24): an inline-SQL guardrail; the upload grace period as a node setting; `private_state` to `local_state` and dropping `Kv`; operation to method in the grant docs; a protocol header catalog, moving the `x-nodule-*` headers out of `service_core_rs`; NSID eradication (audit at `docs/scratch/nsid-audit.md`), its own layer later; a native local-client read path for blobs, added to ADR 051 §2.
- Cut the v1 seed migration (operator question below).
- Historical causal checkpoints log WARN "missing entry encryption key" and return a `MissingDocumentKeys` blocker even for checkpoint ids (`try_causal_decrypt`, `big_repo/runtime2/native.rs` ~1100); for `is_causal_checkpoint_id` ids log at debug and return no blocker. Same family as the relay's warn.
- Least privilege after D54: the nodule service's docs handle still reads the registry drawer (`registry_drawer` in `NodulesConfig`, `big_nodule/lib.rs`), which only the removed envelope read used. Drop the reach and the config field.
- A typed initializer input record (trigger install/upgrade/start; previous and current revision) for ADR 047 initializers; they receive `{}` today.
- A test that an any-JSON record (an `unknown`-valued Lexicon) round-trips through writeDatums/readDatums and `day`.
- `blobs.mintCapability`: a nodule actor with a method permbind on `blobs.put` cannot mint a blobs handle (only the operator or a blobs handle can); the guest-bodies test mints from the harness.
- The core cartridge (ADR 053): node-81 publishes and installs it at boot (defs Lexicon, a routine-less core nodule) and the nodules service refuses changes to it (`CoreCartridge`); node-84 makes its nodule act as `micros` and enables it at install. Next, the micros nodule layer: the guest crate, embedding it, the core grants the node writes at install (dispatch to `openActor`/`attenuateCapability` and the read-only runtime permbind to `listCartridgeGrants`/`awaitCartridgeGrants`), handing the guest its identity capability id, `micros.handles`/`awaitHandles`. The `uses` lowering moves to `big_nodule_core` in node-83; it still needs a wasm build of `big_nodule_core` and its deps for the guest.
- ADR 053 step 1 (`TypeLibraries` passed as a value) is skipped for now: the defs are one compile-time declaration both the binary and the core cartridge read, so passing it through four crates changes nothing. Revisit if a second provided library appears.
- Attach (now ADR 052's micros nodule, 2026-11-24; the list below is claytxt's original write-up): a GUI host using a daemon-held node (claytxt's write-up, `feat/desktop-node`): the daemon ensures micro actors, the GUI channel acts as the micro actor (ADR 037 q2), per-micro config datum, intxt installed by the daemon, durable micro actors. Check whether ADR 037 (b) channel identity is built.
- Host-side collection and document cache with preload hinting for the Clay host (the host warms its cache; nothing lands in a micro's address space until used).
- Concurrent meta-doc creation design (see Standing decisions), and an ADR 019 amendment (it documents LWW today).
- `day` boots with an empty `SchemaRepo`: no user datums without a cartridge.
- Wire surfaces: the datum slot registry (overlaps fail) and the manifest-contents snapshot.
- ADR 047 amendments (`#hosted`, lineage datum, fork grants narrowing, sandbox/volume `localState`): proposed at the end of ADR 047, not accepted.
- ADR 026 assumes one blob inventory per drawer; amend it only if per-participant inventories (intxt D70, unconfirmed by the operator) stand. The drawer-scoped blob read checks every inventory by kind either way.
- intxt's remaining NDK sharp edges: `wit/deps` is gitignored and `x/wit-fetch.ts` needs `wash`, which the dev shell lacks; peering means hand-editing `node.json` (may be claytxt's); `add-member` needs a live connection (keyhive contact cards would allow out-of-band invites).
- D46 survey actions: the audit lane's cabinet-service work list (A-D above); delete the three orphan proptest-regression files and run the live seeds once; guardrails: ban path dependencies, group dependency sections under internal headers, track exported macros in the public API; big_repo `public-api.txt` or its exemption; daybook_*/plug_* removal order (move clay/dayframe off `daybook_types`, decide workflows, port or drop plug routines, drop `daybook_ffi` with `daybook_compose`, then delete); crate renames approved 2026-11-12 (`method_core` to `method_core_rs`, `service_utils_rs` to `service_host_rs`), mechanical, queued.
- `day carts` has only `dev-takeover`: no `ls`, `install`, `enable`, `disable` or `uninstall` (they go through `day x <nsid>`). Tables in `day node status` are hand formatted (old `daybook_cli` used `comfy_table`).
- Keyhive/below-the-cabinet lane items: `wal_checkpoint(TRUNCATE)` every 5 minutes in big_repo maintenance (fights Litestream) and the catch-and-log at `native.rs:2484`; `INSERT OR IGNORE` too broad in `insert_commit`/`insert_fragment`; the conflict-identity invariant test; the keyhive archive loads everything at startup (`keyhive_storage.rs:11` FIXME); the big_repo wrong turns flagged in the archive's D30 block.
- Relay ops: node state inventory (seed, keyring wrapping key, keyhive DEKs, `node.sqlite`, `big_repo.sqlite`, blobs redb); no health endpoint, random QUIC port, no stop timeout; the alpha relay's status showed no connected peers while syncing (not investigated). Lingering and the secret store (approved on mothership, D56) move to its x86 replacement, so the relay survives a reboot there.
- Flakes and diagnoses needing an owner: `big_node` `long_test_seeded_partitions_restarts_and_grants_converge` (seed `0xb160de5eed00001`, `docs.recover()` "capability does not permit" on a restarted node, one failure then 3/3 pass); the guardrails `api` ordering drift since `a844e47c` (`service_core_rs`, `service_utils_rs`: the guard reports set diffs `+0/-0`; re-bless and make the guard report ordering) if still present at the tip.
- Test infra: big_node's guest e2e reads `test_nodule_guest.wasm` from a mbx-managed target that gc can delete; build the guest into a path mbx does not manage or have the test build it.
- Windows work collects in `docs/scratch/windows-todo.md` and lands as one PR on a Windows machine.

## Deferred

- wflow service and durable procedures (D3: durable work goes through a wflow service; convention that durable mutations default to wflow; waits for the wflow rework in `../townframe-p3N`); POSIX providers; triage/processors and big_tasks (D5: tasks decide which node, dispatch decides whether and how locally); network/XRPC exposure; single-instance services.
- Drawer keyhive architecture (D26): revisit against upstream keyline patterns with the Keyline migration (roles at Edit, flat vs nested child drawers, rotating roles, steward pattern).
- Cabinet read/write options (2026-11-06): a gate-verdict cache keyed by datum heads, `readDatums` patch mode and splice input for Text, client-held Automerge, Automerge 0.12 authors, two-phase write. None is a pure win yet; no caches before the low-hanging costs. Details in the archive's Deferred block.
- Draft (unsigned) branches (parked by the operator); signatures in Automerge `extra_bytes`; node-attested attribution instead of datum signatures (needs an ADR 020 amendment).
- A virtual `wasi:filesystem` for guests (ADR 051).
- Not offering node-local methods to other nodes at all (ADR 042 §3 only refuses at call time): mount them only on local transports' routers vs one router filtering by origin; ties into a "list providers/methods" operation and network exposure.
- Shelved to this week: the atmosphere border and dpaths (above).

## Open questions for the operator

They will come back to these.

- **ADR 051:** are micros' collections public by construction, and does revocation reach a running instance?
- **CLI:** Ctrl-C exit codes (130 for commands, 0 for a foreground server), and dropping the build time from `--version`.
- **Cut the v1 seed migration?**
- **Sisters and grants (ADR 038, D56):** a grant datum syncs through the shared config drawer but the node-local grant to each node's service actor does not; who grants it on a sister?
- **Sharp-edges (#249):** ids on `UnrelatedHistory` and `Conflict` (recommended yes); keep the `CommandId` name (recommended); location/backtrace only under RUST_BACKTRACE.
- **The atmosphere border (shelved):** run the spaces experiment and review draft ADR 048 (three tiers, inward direction).
- **Not offering node-local methods** (see Deferred).
- **Remote readers over TCP (intxt C16/C18):** a sturdyref (a durable bearer handle) for the web viewer, or an admitted viewer actor that authenticates its channel and holds a durable grant? The nodules lane recommends the actor, plus C17's anonymous public reads.

## Archive index

`nodule-runtime-ledger-archive.md` holds, verbatim and in the original order (headings are dated 2026-11-24):

- D0-D13 and plan P: dispatch contract sketch, manifest shape, slice decisions, Wasm provider design, init, plugs/config/security direction, Clay's needs, the registry and malleable-plug decisions, the P1-P7 big_plugs plan.
- D14-D22, plan N1-N6, D3-D6: the node PR plan with its pinned findings and root causes.
- D23-D30: the keyhive CGKA fix, install-by-capability, scale and cost tables, the datum gate cut, the big_repo wrong-turn log and perf landing notes.
- "In flight" as of 2026-11-12: ADR 037/042/044/045/047 landing notes, boot-cost measurements, the #84 freeze.
- "Done" through 2026-11-06: P2-P6, init, slices 2a-2c, dispatch slice 1, the wasmCloud fork seam, review-fix batches.
- "To do (this PR)", the merge follow-ups, and the 2026-11-17/18 follow-ups (D50-D58 in full, plus filed todos).
- The 2026-11-19 operator session and slice follow-ups.
- D31-D49: transports, datum model creep, CLI answers, rename, big_ndk week, D39 headers, D40 manifests, D41 method Lexicons, D42 configuration, D43 client identity, D46 review survey, D47-D49.
- The old "Deferred" and "Open questions" blocks, including every settled question.
