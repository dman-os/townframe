# intxt alpha: what the node side must deliver

Status: Draft
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: The top-down grounding for this week's node work (ledger D38). It derives the node, `big_ndk`, `day` and relay requirements from the intxt alpha's user journeys, and it is the boundary between the node lane (this one) and the intxt lane: the intxt lane owns intxt's datums, micro and cartridge contents; the node lane owns everything those stand on.

## The alpha, as the operator scoped it

- intxt is a product shipped as a micro (inside a cartridge). Its first job: the operator reviews the PRs agents write, on their phone. Reviews live in drawers.
- Every device runs its own node, the phone included. Users push intxts through their own node to a **relay acting as their mother node**. Relays are nodes anyone can run; the alpha needs no sponsorship.
- **Web viewing goes through webnodes** (updated 2026-11-10: upstream keyhive is unblocked, and a separate Opus lane delivers webnodes). The intxt micro runs in the browser on a throwaway webnode that fetches from the relay, so intxt needs no separate non-micro web viewer.
- Most of intxt is not GUI: agents produce and annotate intxts, a CLI drives them, and MCP servers expose them. Whether agents reach intxt as a **harness extension** (a Claude Code plugin bundling the MCP server, skills and hooks, e.g. publishing a review when a PR is opened) or as a **CLI tool they invoke** is part of the project; probably a mix.
- **The intxt nodule exists and the node lane owns its design** (operator, 2026-11-10): intxt needs custom code supporting the micro and agent annotations. The intxt lane owns the datums and the micro.

## Journeys

**J1. An agent publishes a review.** On the dev machine, an agent (Claude Code) finishes a PR and creates an intxt: the diff, its own annotations, verification results. It talks to the local node through MCP or the `day` CLI.

**J2. The review reaches the relay.** The dev machine's node syncs the intxt drawer to the user's relay, which keeps it available while the dev machine sleeps.

**J3. The user reviews on the phone.** The phone's node syncs from the relay (and directly from the dev machine when both are up). The intxt micro renders the review; the user annotates and approves; their writes sync back, and the agent sees them (it waits on the review through MCP or the CLI).

**J4. The user shares a review publicly.** The user marks the drawer public. The relay serves it to anonymous web clients, and the web app renders it at a shareable link.

## Requirements by journey

### J1: authoring from agents and the CLI

| # | Requirement | State |
|---|---|---|
| R1 | A cartridge carrying intxt's datum Lexicons and a nodule can be installed and enabled on the local node | Built for test fixtures (`big_plugs`, enable registers Lexicons, D37); no `day` porcelain |
| R2 | **`big_ndk`**: the intxt nodule is written as typed `Handler<M>` impls, calls docs/drawers/blobs through typed clients, and is unit-testable natively | Not started (the plan's NDK v0) |
| R3 | **`day cart pack/install/enable`** from built wasm plus manifest; `day` never builds source (D38) | Not started |
| R4 | **`day mcp`**: a stdio MCP server whose tools are the methods the presented capability reaches, schemas and descriptions from Lexicons | Not started |
| R5 | **Generated `day` subcommands** for installed cartridges' methods, from Lexicons; `day exec`/`x` as the generic form | `day call` exists; rename and generation not started |
| R6 | **Method Lexicons with input checking** (D31): R4, R5 and R10 all read them | Not started |
| R7 | **Narrowed capabilities from outside the node**: an agent's MCP session gets a capability scoped to intxt's methods and drawers, not the operator's full one | One missing entry in `register_built_ins` (dispatch's own service) plus a test; awaiting operator OK |
| R8 | **Large inputs**: a diff can exceed `MAX_MESSAGE_OCTETS`, so big payloads go through blobs and the method takes a blob reference | Blobs exist; the method convention is open |
| R19 | **Standard WASI for nodules, starting with the filesystem**: a nodule declares the directories it needs, the user grants specific host directories at enable, and the node mounts them as WASI preopens. The intxt nodule can then read a git checkout itself instead of the agent pasting diffs. wash-runtime already does the mechanism (`LocalResources::volume_mounts`, opened as preopens with `FsPerms` in `engine/volumes.rs`); `big_nodule_wasm` passes no volumes, limits or environment today | Requirement and grant vocabulary not designed; the passthrough is small |

### J2: the relay as mother node

| # | Requirement | State |
|---|---|---|
| R9 | **A relay composition**: a node crate with no user, a TCP listener, a peer policy, and a bootstrap identity | Generic `big_node` supports it (ADR 033); the composition and its ADR don't exist |
| R10 | **Mirroring private drawers to the relay**: the user adds the relay's identity to the drawer's group with keyhive `Access::Relay` (sync bytes, can't decrypt) | keyhive has the level; the user-side action and the relay's mirror policy are not wired |
| R11 | **Peer admission for the alpha**: the relay accepts the peers it is configured to accept (an allowlist in its config), no sponsorship | Peer policy is an embedder hook (ADR 033); the relay's policy is not written |

### J3: the phone

| # | Requirement | State |
|---|---|---|
| R12 | The phone runs a node (Android) and syncs from the relay and from sibling nodes | Dayframe Android lane (#81); node-on-phone composition is the user-node ADR |
| R13 | The intxt micro reads and writes intxt datums through its node | Micro access to node services is deferred until Clay runs on the node (plug-authoring Sequencing 4); intxt forces it into this alpha |
| R14 | **Waiting on a review**: an agent blocks until the user responds, through `PollDatums` long-poll behind an MCP tool or a CLI command | `PollDatums` exists; exposing a long wait through MCP is open (Q4) |

### J4: public reviews on the web

With webnodes (2026-11-10), the browser runs a throwaway node that syncs public drawers from the relay as a peer, so R16 and R17 are no longer needed *for the web viewer*. R17 stays useful for plain HTTP clients; R16 stays relevant if the relay serves non-node clients.

| # | Requirement | State |
|---|---|---|
| R15 | **Making a drawer public, the app-public way.** Upstream keyhive removed the "World Public" principal (keyhive PR #284, Automerge's November 2026 post): it was special-cased everywhere and let anyone ask a sync server for every public document. Public is now ordinary capabilities scoped to one app. Two principals stand for the app: an *app reader* is granted `read` on the content, and it grants `relay` to an *app fetcher*. The app ships the reader's X25519 secret (so it can decrypt) and the fetcher's Ed25519 signing key (so it can pull ciphertext from a sync server), but not the reader's signing key, so nobody can write. For intxt: "make this review public" grants `read` to the intxt app reader; a throwaway webnode signs in to the relay as the intxt fetcher and decrypts as the reader. The relay still sees only ciphertext, and the fetcher can pull only what was made public *to intxt*. Anyone with read access can do this, not only the owner. Upstream has no API for it yet ("set up by hand"). | Our fork still has `Public`; it goes away when we move to upstream. Where the app's two key pairs live (in the cartridge? a cartridge-scoped actor, ADR 022?) is the consent/permissions/actors topic again |
| R16 | **Anonymous reads on the relay's TCP listener** are limited to public drawers | The listener decides what an uncredentialed request may do (`Anonymous`); a public-drawer policy is not written |
| R17 | **Query methods over `GET /xrpc`**: the web app reads with XRPC queries; query parameters need the method's Lexicon to type them | Unimplemented (`big_dispatch/xrpc.rs` serves POST only); lands with R6 |
| R18 | The relay can serve intxt datums without the intxt cartridge: unknown datums are readable as best-effort raw data; only writes need the schema (`big_cabinet/schema.rs`) | Holds today; whether the web app wants the relay to validate (and so carry the cartridge) is Q5 |

## What this says about relays and nodules

- **The alpha relay runs no intxt nodule.** Everything it does is generic: sync, store, serve public reads. Nodules on relays become interesting once a relay acts *for* users (rendering, search, notifications), and that is where the open authority question lives: a relay nodule acting for many users needs a capability per user or per drawer, bounded by what each user granted the relay. That is the consent/permissions/actors topic (D37, this week), and the relay is its first concrete case.
- **The relay's access is the user's grant**: `Access::Relay` for private drawers, `Public` for shared ones. The relay never holds a user's capability beyond those.
- **Unknown datums reading raw** means relays can stay cartridge-agnostic. Whether they should validate what they serve is a choice, not a need.

## Order of work, top down

1. **Sketch the intxt nodule against the `big_ndk` API we want** (with the intxt lane's datums, or stand-ins until they exist). The sketch decides the NDK surface.
2. **`big_ndk` v0** to make the sketch compile and run on the node: R2, then R1 through test fixtures.
3. **Lexicon-backed methods**: R6, then R17, R5, R4 as consumers.
4. **The relay composition** (R9–R11, R15–R16), with its ADR, alongside the day node composition.
5. **`day cart` porcelain** (R3) and narrowed capabilities (R7).
6. **WASI filesystem grants for nodules** (R19), as soon as the intxt nodule sketch shows the shape of its directory needs.

R12 and R13 belong to the Dayframe and Clay lanes; this doc only records that intxt needs them.

## Open questions

- ~~Q1~~ Resolved (operator, 2026-11-10): intxt has its own nodule, designed by the node lane.
- ~~Q2~~ Direction (operator, 2026-11-10): nodules get standard WASI, including granted host directories (R19), so the intxt nodule can read a checkout. Passing diffs as blob references stays for agents working without a granted directory.
- **Q3.** Is the intxt data model being written somewhere this lane should read?
- **Q6.** Answered by upstream's redesign (R15): a throwaway webnode syncs as the intxt app fetcher and decrypts as the intxt app reader. Left for the webnodes lane: how a webnode is given the fetcher and reader keys, and the relay's peer policy (R11) admitting the fetcher.
- **Q7.** Moving to keyhive 0.6 (Keyline authority graph, late-bound capabilities, `keyhive_codec`, causal encryption in `keyhive_content`) is a migration under big_repo. Who owns it: the webnodes lane, or its own lane?
- **Q4.** MCP tools are request/response. Waiting for the user's review is either a long-polling tool call or a resource subscription; which one agents handle well is to be tried.
- **Q5.** Should the relay validate public intxt datums it serves (carrying the intxt cartridge for schemas), or serve them raw?
