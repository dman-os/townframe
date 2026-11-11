# ADR 038: Node orientations and the relay alpha

**Status:** Accepted direction (operator, 2026-11-11). The names "mother" and "sister" are provisional.

**Related:**
- [ADR 033: what a node is](033-what-a-node-is.md) (the peer policy hook; one ADR per composition)
- [ADR 037: holder-bound capabilities and channel identity](037-holder-bound-capabilities-and-channel-identity.md)
- [intxt node requirements](../scratch/intxt-node-requirements.md) (R9–R11, R15)

## Context

ADR 033 made `big_node` a generic node: the embedder supplies the policy for whom to dial, whom to accept and which partitions to sync. User nodes and relay nodes are compositions in their own crates. The intxt alpha needs a relay: reviews written on a dev machine must reach the phone while the dev machine sleeps, and a throwaway webnode views public reviews through it.

The end state runs relays inside wasmCloud with btress accounts, sponsorship, quotas, backups and scaling. Starting there would be building the stack before knowing what it needs. The relay is first a **relationship** between nodes; the stack that runs it at scale comes after.

## Decision

### 1. Per drawer, only keyhive's access levels

Whether a node holds a drawer is expressed with keyhive's standard verbs on the drawer's group: `relay` (sync and store ciphertext, cannot read), `read`, `write`, `admin`. There is no drawer-level relay concept of our own. Public content uses upstream keyhive's app-public pattern (an app reader with `read`, granting `relay` to an app fetcher), not a public principal.

### 2. Orientations are node-level and user-facing

A user relates whole nodes, not drawers:
- A **mother node** is where the user's drawers are backed up by default. Creating a drawer on any of the user's nodes adds each mother node to the drawer's group with `relay`.
- A **sister node** is another of the user's own devices. A new device copies its configuration (peers, mother nodes, preferences) from a sister.

The orientation is a property of the user's set of nodes, so it lives in data the user's nodes share (a synced configuration drawer, ADR 033's local drawers), not in one node's `node.json`. Per drawer, the orientation becomes ordinary keyhive membership, so overriding it for one drawer is just changing that drawer's members.

### 3. The relay alpha is a native node in relay mode

- **What it is:** a `big_node` composition started by `day` (`day node init --relay`, then `day node run`), running natively on mothership. No wasmCloud and no btress in the alpha.
- **Peer policy:** accept any peer that connects; mirror every drawer the relay is a member of (with `relay` or more). This replaces ADR 033's mirroring half (`Drawers::mirrored`) with membership-driven mirroring; the access half (`groups_with_member`) still bounds it.
- **Authentication:** none at the account level. keyhive membership still decides what the relay syncs, and with `relay` access it stores ciphertext it cannot read. The exposure is storage abuse (anyone can make it hold their drawers), which is acceptable for an alpha we run ourselves.
- **Public reads:** a throwaway webnode connects as the app fetcher (the webnodes lane) and decrypts as the app reader.

### 4. What comes later, on the same relationships

btress accounts and sponsorship, quotas, backups, scale-out, and the wasmCloud deployment are layers over this relationship model. They change who may make a node their mother, and how a relay is run, not what a mother node is.

## Consequences

- The user-node composition needs "add a mother node": dial it as a peer, record the orientation in the shared configuration, and add it with `relay` to new drawers, plus to existing drawers when the user asks.
- The relay composition needs its peer policy (accept all) and membership-driven mirroring. Both fit ADR 033's embedder hook.
- intxt's journey J2 (the review reaches the relay) is: the dev node's drawer has the relay as a `relay` member, so it syncs there.

## Open questions

1. The names "mother" and "sister".
2. Whether adding a mother node backfills existing drawers by default, or only new ones.
3. Removing a mother node: is it dropped from every drawer's group (keyhive revocation), and what happens to its stored copy?
4. How long a relay keeps data for a drawer that no longer has it as a member.
5. Storage limits on the alpha relay, if abuse shows up before accounts exist.
