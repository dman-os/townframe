# FDR 002 vocabulary: disposition for review

Source: `docs/fdrs/002-vocabulary.md`. Checked against `docs/fdrs/004-workspace-cli.md`, `docs/dict.md`, and `feat/drawers` FDR 001 and ADRs 014–015. This is an issue ledger, not a replacement or authority to edit `docs/`.

| Current section | Keep / revise | Why |
| --- | --- | --- |
| Opening review history and old→new name log | Remove from normative prose | Explain the vocabulary directly, not how the conversation arrived at it. Historical mapping can be a migration note. |
| Context: repo is not one shareable unit | Keep | Distinguish local node from document, drawer, and checkout. Drawers branch further separates a known drawer from a mirrored drawer. |
| §1 entity ladder | Replace hierarchy | A drawer is a document/collection that can span nodes and include documents in several drawers; a checkout can select across drawers and path sources. Neither is a child of one node in the semantic model. A node has local policies and checkouts, not ownership of every referenced drawer/document. |
| §2 one agent per node | Verify against newer identity/authority model | `feat/drawers` Cabinet expressly does not define user-facing principals as Keyhive IDs. Keep node/auth distinctions but do not bake a keyhive type lattice into the user hierarchy without evidence. |
| §2 “adding a device = mirroring a node” | Revise | New node gets its own identity and selects drawers to know/mirror, not a clone or complete copy of another node. No keys copied remains a useful constraint. |
| §2 node metadata: replicated universally, membership registry, relay hints | Superseded by ADR 014 proposal | Shared known-drawers doc and per-node configuration document; another node's retention policy is advice, not this node's command. Access, listing, local retention, and bytes are separate. Node-registry/my-devices assertions require a separate identity/recovery owner. |
| §2 unnamed node vs FDR 004 `db init [name]` | Contradiction to resolve | Separate device/node display name, filesystem directory, and drawer display name; determine which users see and which sync. |
| §3 `.dnode`, `.dtree`, nested checkout safety | Retain semantics, check promises | Do not claim every node directory is non-copyable because of keyring/encryption without checking actual recovery model. `.dtree` carries durable local bindings. Names are interface decisions if agreed. |
| §4 drawer as share unit and permission boundary | Revise using drawers FDR | Drawer ID is descriptor document ID; authoritative roster membership is separate from effective Keyhive authority, bytes, availability, and local mirror policy. A document may occur in several drawers; branches are separate documents and are not included automatically. Single-document drawer may grow without ID change. |
| §4 ephemeral checkouts | Retain concept | An ephemeral checkout has temporary state; do not call it a durable checkout with no bindings if the store actually uses temporary bindings. |
| §4 contact | Retain as distinct person/contact notion | No assumption that a person maps to one node, one Keyhive agent, or one drawer. |
| §5 Keyhive agent/principal taxonomy | Move technical lattice to authority ADR | Give a short definition of node's signing/sync authority; don't define Cabinet principals as Keyhive IDs or declare every drawer a Keyhive document agent. |
| §6 relay account | Retain distinction | Relay account, relay service, relay node, and retained bytes differ. ADR 014 separates sponsorship request, grant, relay acceptance, and byte retention. |
| §7 CLI context, no login verb, sync service | Retain intent; verify FDR 004 | Context lookup and explicit CLI service are interface promises. Signing into relay account in §8 does not imply general CLI login. Read-only commands must not publish. |
| §8 new-device flow | Rewrite | Admission/knowledge/mirroring/availability are not a single hydration step; metadata need not download a collection. Peer/relay only candidate providers. |
| Resolved/open/backlog | Replace with genuine open decisions | Node display names, migration vocabulary, contents of shared known list, node policy, auth/identity boundary, directory layout. |

1. **Decided:** mirror applies to a drawer's selected data/metadata on another node; granting authority and acquiring bytes are separate states. Creating another of your own devices is a product onboarding operation, provisionally **link device**, not “mirror node.” Its identity/authorization flow needs separate design.
2. **Decided:** `db init` creates a node, its home drawer, and a root checkout selecting only that home drawer. Adding other drawers does not silently expand the root checkout. Node configuration may later live in documents/drawers, but that storage layout is not a prerequisite for the vocabulary.
3. Does a node display name identify a device, its local directory, or a replicated person-owned node identity? Is that name shown on other devices?
4. Is `checkout` scoped to a single node's local document store but allowed to select across drawers and `/by-id` paths? How is its default drawer selected?
5. Do we still want `.dnode`/`.dtree` as public layout and CLI context markers, while treating their exact local storage and keyring rules as ADRs?
