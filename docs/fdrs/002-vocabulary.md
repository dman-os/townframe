# FDR 002: Vocabulary — Nodes, Drawers, Checkouts, and the Identity Ladder

**Status:** Draft. Resolved-in-review items are folded into the body; the
remaining open calls are at the end. This document replaces the `Repo` section
of `docs/dict.md` once ratified. Note: even the app name **daybook** is not
final; the `db` CLI binary name is locked per review.

**Old name → new name:** ~~repo~~ → **node**. The ~~home~~/~~profile~~
proposals and the transient `.dhome`/`.dctx` dir names lost the review; the
node's directory is **`.dnode`** (the git-dir). Everything else below is
either retained or newly defined.

---

## Context

The word **repo** is overloaded and provably wrong at this point:

- It implies git semantics (one shareable unit of data + history). Reality:
  the **shareable** unit is a smaller collection (drawer, or even a single
  doc); devices sync drawers, not "the repo"; two people's data *overlaps
  through shared drawers* rather than being fork/cloned wholes.
- In the codebase it labels two different things: `big_repo` (the local
  storage + sync-node context bound at startup) and `daybook_core/repo.rs`
  (an identity/auth context). Neither is "a repo" in any git sense.
- It carries no identity connotation, yet the decisive startup binding is
  exactly an identity(ish) set: which keyhive agent a sync connection uses.

The user-facing world it must express (from the normie/GUI discussion): you
open the app, you have a default drawer to put things in; to collaborate you
create more drawers; GUI lists objects **grouped by identity context**; your
own devices sync your stuff automatically, relays extend reach, and there is
no user-account → drawer hierarchy *mandated* — hierarchy is a presentation
choice, not a structural constraint.

Prior art for terms: **Keyhive** (agents vs principals vs identity — see §2;
edge names/petnames for the naming layer; Beelay for sync); **Patchwork**
(no repo noun at all — a per-device account doc, contact docs, folders);
**jj** (repo vs workspaces layering); Syncthing (devices + shared folders as
first-class normie vocabulary — and its node id is exactly the mental model
we adopt); Signal (linked devices).

---

## Decision

### 1. The entity ladder

```
identity (deferred, external layer; petnames/edge-names shaped)
  → normie words: contact / person
  └─ keyhive agent  = authority-graph entity that can sign
                      Document :< Stateful :< Stateless   (see §2)
  └─ node           = a daybook sync instance bound to ONE keyhive agent;
                      holds drawers, node metadata, local stores
                      ├─ drawer    = shareable collection + permission
                      │              boundary (KEEP)
                      └─ checkout  = durable materialized surface with
                                     bindings and transactional state
```

### 2. The node (renames ~~repo~~)

The **node** is the thing we used to call the repo: one identity context's
sync instance.

- **A machine may run multiple nodes**; a node syncs *through a single
  keyhive agent* (its own). Every node — including the node on your own
  second device — **participates in sync with its own identity, using the
  same machinery as cross-user syncing**. There is no special "self-sync"
  path.
- **Adding a device = mirroring a node.** A new node is created on the new
  device and mirrors an existing node's content **through sync, not by
  copying**: the new node mints its own principal and is *admitted* into the
  relevant drawer/group agents. Private keys are **never moved** between
  nodes. Copying a `.dnode` directory does *not* work and must not be a
  supported migration: local stores are encrypted at rest, content keys live
  in the OS keyring, and signing keys are device-bound. Everything needed to
  re-materialize arrives via the keyhive graph (capability admission) and
  normal sync.
- **node metadata** (formerly "meta drawer") is the replicated core state
  every node mirrors: drawer memberships, the node registry ("nodes that
  have these drawers", with relay hints — relays are just long-lived nodes),
  and the my-devices list. It is user-invisible in v1 but load-bearing for
  hydration; its contents spec lands in FDR 004 and the reconciliation ADR 011.
- The node is **not user-facing-named**: nobody names their node; normie UX
  says "mirror your node" or just "add device". This is deliberate — techie
  word, normie flow.

### 3. Directory surface names

- **`.dnode`** — the **home of a node**: the node's local directory holding
  secrets, local stores (automerge/blob/sqlite), node-metadata cache, and
  sync config. The "git-dir" of the layout; the *home-of-a-node* reading is
  why this beats `.dctx`, and `$HOME` is the borrowed intuition. **Nodes
  live wherever the user puts them** — there is no registry-only location:
  `db init` creates a fresh named directory **in the cwd** containing the
  node's `.dnode` (plus the root checkout's `.dtree`), the GUI defaults to
  creating them under `~/Documents/`, and an in-tree `.dnode` marks a node
  rooted at that directory. Which node a CLI invocation talks to is
  **context-dependent** (cwd resolution; see §7); an app-side database may
  additionally list/track all local nodes for convenience (extra, not
  required).
- **`.dtree`** — a checkout's local state (bindings, transactional store) per
  the pauperfuse checkout model; present in *any* checkout directory,
  including adopted ones; records which node it belongs to. `.dtree`s are
  checked out from `.dnode`s.
- Nesting: `.dtree` inside materialized subtrees is not re-adopted; a
  `.dnode` nested inside an adopted tree must be refused or re-bound (FDR
  001 §5; pauperfuse ADR 010/011 detail). A `.dnode` is always more than a
  folder of files (keyring-bound material) and is never treated as data to
  copy.

### 4. Drawers, checkouts, contacts

- **drawer** (kept): the share unit, the permission surface, the thing the
  GUI presents and the CLI operates on. `db init` creates a node plus its
  **root drawer** (default drawer; the normie's first stop).
- **checkout** (kept; **~~view~~ removed per review**): the only
  materialization surface name. Stateless projections (wasi runs, FUSE
  mounts, `db export`) are just **ephemeral checkouts** — same machinery, no
  durable bindings.
- **contact** (kept): the user-facing card for *someone else's* identity
  (name/avatar/color — Patchwork's contact-doc precedent). Owns the future
  addressable-identity layer slot; **petnames/edge-names** (Keyhive's
  `edge_names` design) are the obvious substrate when it lands.

### 5. Keyhive alignment: agent vs principal (per keyhive design)

Adopted verbatim from Keyhive's `group_membership` design:

- **Agent** = an entity in the authority graph capable of receiving,
  delegating, and exercising authority; keyed by a root key pair. Subtyping:
  **`Document :< Stateful :< Stateless`**.
  - **Documents and groups ARE agents** (document agents carry content ops +
    auth ops; groups are stateful agents). So "principal also includes docs
    and groups?" resolves as: the graph entities are all agents; drawers and
    docs will be document agents.
  - **Stateless agents** are bare pubkeys — device keys, hardware keys,
    passkeys; the leaf signers. Not rotatable; device keysets, rotation, and
    multi-device support are managed by **stateful** agents.
- **principal** (the notebook's word) = the cryptographic authority behind
  agents. Daybook docs use **agent** when talking about the graph, and keep
  "principal" only where ADR 006 established it (recovery principal, repo
  agents). **device** stays the normie word for the stateless agent running
  on one machine.
- A daybook **node** syncs through **one** keyhive agent; identity (the
  real-world binding) remains external, and addressable identity stays
  deferred with petname-style naming as the eventual layer.

### 6. Relay account

The business/sponsorship relation at a relay (ADR 05) correlating a set of
agents. Distinct from node and identity; a node may use several relay
accounts, and one relay account may serve several nodes. Relays know agents
and sponsorship, not nodes or drawers by name.

### 7. CLI context model (no logins)

- The CLI has **no login verb**; it is a contextual tool.
- Context resolution: nearest ancestor directory with `.dtree` (which names
  its node) or `.dnode` → `DAYBOOK_NODE` env → app-configured default (app
  global config / XDG config) → error with a hint. The CLI is
  context-driven; a default node exists only through explicit configuration,
  never implicitly.
- `db status`/`doctor`-style introspection prints the resolved context
  (node, agent in use, drawers in scope, checkout state).
- **Sync server**: apps start one per node by default. From the CLI it must
  be started explicitly (`db sync serve` — foreground, daemonized, or as a
  systemd unit). This is the *only* CLI surface where "standing a sync node
  as an identity" appears explicitly.

### 8. New-device flow = mirroring (normie-first)

1. Install → **create node** (mint device agent, root drawer, `.dnode`) or
   **mirror an existing node**: sign in to a relay account (hydrate node
   metadata; ADR 06 recovery on total loss) *or* p2p-link from an existing
   device (QR/link; the old device **admits the new node's agent** into the
   drawer group graphs — no secret bundle is transferred).
2. Hydrate node metadata through sync: drawer list, node registry (relays
   *and* my other devices), my-devices list → direct device-to-device p2p
   sync is **first-class, on by default** (Syncthing-shaped; for relayless
   users it is the entire sync story, and it starts working the moment both
   devices are on — both nodes participate with their own agents over the
   same machinery as cross-user sync).
3. Checkouts rebind on read; content syncs drawer-by-drawer by object
   identity (FDR 001 presentation-only divergence rules apply).

The CLI never *requires* a relay: nodes and device sync work fully p2p.

---

## Resolved in this review (recorded so the history is explicit)

- ~~repo~~ → **node** (concept); the node's local directory is **`.dnode`**
  ("the home of a node"), replacing transient name candidates (`.dhome`,
  `.dctx`). Normie phrasing: "mirror your node". `home` does not survive as
  a concept noun — it is filesystem surface, nothing more.
- ~~view~~ — removed; ephemeral checkouts cover it.
- **meta drawer** → **node metadata** (replicated core state).
- Agent-vs-principal settled from Keyhive's design: documents and groups are
  agents (`Document :< Stateful :< Stateless`); device keys are stateless
  agents; "principal" survives only as ADR 006 vocabulary.
- Adding a device never copies secrets; `.dnode` is keyring-bound,
  encrypted-at-rest material that cannot be copy-pasted between nodes.
- `db` stays the CLI binary name (daybook itself is still an open name).

## Open calls

1. **Node naming** — nodes created from the GUI (under `~/Documents/`) will
   need user-facing names; device names as the default is the current idea.
   Does the CLI's `db init` also take a name (defaulting to the created
   directory's name), and do node *display* names live in node metadata
   facets? _Blocks: FDR 004 CLI._

### Resolved in this review

- ~~XDG node layout~~ — **nodes are created anywhere**; the in-tree `.dnode`
  *is* the node (that's the point of the name). CLI `db init` creates a new
  dir in the pwd for it; the GUI creates nodes under `~/Documents/`. This
  removes the default-node-selection problem for CLI usage: **the node is
  context-dependent** (nearest `.dtree`/`.dnode` in cwd ancestry, then
  `DAYBOOK_NODE`), and a default/registration layer is an app-side extra
  (app db), not a CLI requirement.
- ~~Ephemeral-checkout plumbing~~ — **every checkout, durable or ephemeral,
  owns its own state**; that is exactly what `.dtree` is for. Ephemeral
  checkouts get throwaway `.dtree`s and no special plumbing.

## Backlog items landing in other documents

- `big_repo` → **node store** (or node context) and `daybook_core`'s Repo
  rename: **code refactor**, not blockable by FDRs; do it in the same pass as
  sketch cleanup so vocabulary stays honest.
- Node metadata contents spec (drawer memberships, node registry, device
  list, relay hints): FDR 004 (workspace CLI) / ADR 011 territory.
- Recovery/add-device details: ADR 006 stands; the mirroring flow above
  defers to it for total-loss cases.
- Tree/store/adapter naming for pauperfuse internals: ADR 010.
- Addressable identity and petnames: future FDR; contact cards (§4) reserve
  the slot.
