# P2P requirements and forcing functions

Status: Draft for the operator.
Owner: Townframe maintainers
Updated: 2026-11-12
Scope: What every P2P subsystem asks of a node relationship (D42 "Sisters and mothers"), before the relationship vocabulary is named.


**Builds on:** [ADR 038](../adrs/038-node-orientations-and-the-relay-alpha.md), which already settled two things:
- Per drawer, keyhive's access levels (`relay`, `read`, `write`, `admin`) are the only vocabulary.
- Orientations (mother and sister) are node-level, live in configuration the user's nodes share, and are compiled down to keyhive membership per drawer.

This doc asks what those orientations have to express, by going through every subsystem that moves data or work between nodes.

## Forcing functions

Each entry says what moves, who must hold it, what trust that takes, and what the node has to afford.

### F1. Document sync (big_sync, keyhive)
- **What moves:** Automerge documents in drawers, plus keyhive membership and CGKA ops.
- **Who holds it:** every member of the drawer's group.
- **Trust:** `relay` members hold ciphertext; `read` and above decrypt.
- **Resources:** storage grows with history. A phone may not want every drawer's full history (ADR 019 selective mirroring).
- **Today:** membership-driven mirroring on the relay (ADR 038 §3); `MirrorPolicy` on user nodes.

### F2. Backup and availability
- **What moves:** the same documents, so that they survive a lost device and are reachable while every user device sleeps.
- **Who holds it:** an always-on node (the relay or mother) that need not read anything.
- **Trust:** ciphertext only (`relay`).
- **Resources:** always-on, storage-heavy, and bounded by quotas and abuse limits (ADR 005).
- **Today:** a mother node is added with `relay` to new drawers (ADR 038 §2). Backfilling existing drawers is ADR 038's open question 2.

### F3. Blob placement
- **What moves:** blob bytes, which are much larger than documents and are encrypted (ADR 026 §1).
- **Who holds it:** whoever pins them through inventories (ADR 001, 026), where the pin decides the placement.
- **Trust:** ciphertext; the JWK travels in the doc.
- **Resources:** this is where devices differ most. A phone on a metered link should fetch on demand, while a mother keeps everything.
- **Requirement:** placement defaults per relationship and per node capacity, separately from document sync. The same drawer can be fully synced but have its blobs only partially mirrored.

### F4. Plaintext processors (triage, indexing, ML)
- **What moves:** work, not data. A processor reads plaintext and writes derived datums (ADR 010).
- **Who holds it:** a node with `read` or above on the drawer, capable of the work.
- **Trust:** plaintext, so a relay or mother is excluded by construction.
- **Resources:** CPU, memory and battery. Heavy processors belong on the laptop, not on the phone.
- **Requirement:** a node must advertise what it can afford (always-on, compute class, on battery), and a processor's placement default must combine that with plaintext access.

### F5. Task coordination (ADR 011)
- **What moves:** task tickets, router election and executor registration over BigSync.
- **Who holds it:** nodes that opt into an executor pool.
- **Trust:** pool membership, which is a group in its own right.
- **Requirement:** the same capability advertisement as F4; election should prefer always-on nodes.

### F6. Configuration that follows the user
- **What moves:** cartridge settings and grants (decided in D42: one document per cartridge), plus the orientations themselves, preferences and peers.
- **Who holds it:** all of the user's own nodes (sisters).
- **Trust:** plaintext, and the same principal.
- **Requirement:** a new sister copies from an existing sister (ADR 038 §2). Per-node values, such as which cartridge is enabled and at which version, stay out of the shared documents (D42).

### F7. Cartridge distribution
- **What moves:** published cartridges (registry drawer) and their wasm blobs.
- **Who holds it:** every node that installs one. A sister should be able to install whatever its sister has, without re-publishing.
- **Requirement:** follows from F6 plus F3. The install reference (a strongRef with heads) must resolve on the sister, so the registry drawer has to be synced there.

### F8. Recovery
- **What moves:** keys and identity (ADR 006 recovery principals).
- **Who holds it:** a recovery principal, possibly anchored on the relay.
- **Requirement:** whether a mother may anchor recovery is a relationship property with higher trust than `relay`.

### F9. Reachability and relaying of connections
- **What moves:** connections, not data: dialing, NAT traversal and bootstrap tickets.
- **Who holds it:** whoever is reachable. Mothers and relays are the default rendezvous points.
- **Requirement:** independent of data trust. A node can relay connections for drawers it doesn't hold.

### F10. Public reads (intxt reviews)
- **What moves:** public content to throwaway clients (webnodes).
- **Who holds it:** an app fetcher with `relay` and an app reader with `read` (ADR 038 §1).
- **Requirement:** this is not a relationship between the user's nodes, so it stays out of the orientation vocabulary.

### F11. Inbound admission
- **What moves:** nothing yet. This is whether a node answers at all: a connection, a dial, an iroh ping.
- **Who may reach a node:** today, only peers the node expects. A node that isn't a relay accepts inbound connections only from its sisters. A relay accepts the members it holds drawers for. Later, peers holding a ticket the node issued.
- **Requirement (operator, #84 review, 2026-11-12):** a node never replies to a peer it doesn't expect, iroh pings included. Today's peer policy (`big_node/peers/policy.rs`) doesn't enforce this. The relationship vocabulary has to say who may reach whom, and tickets are the extension point for reach-outs beyond it.

## What the list says

The operator's phone example (two phones as sisters shouldn't mirror everything; heavy processors must not run on the phone or the relay) shows that one word is doing two jobs:

1. **Relationship (who the node is to me):** same principal versus another principal, and how much trust: ciphertext, plaintext, or recovery anchor. This decides *eligibility*: may this node hold or process this data at all? This is what keyhive membership compiles from.
2. **Profile (what the node can afford):** always-on, storage class, compute class, battery or metered network, and reachable from outside. This decides *defaults among eligible nodes*: should it hold all blobs, run the heavy processor, host the task router?

Proposal: keep **mother** and **sister** as relationships only.
- *mother* = another node trusted with ciphertext, the backup and rendezvous default (F2, F9).
- *sister* = the same principal, trusted with plaintext, configuration follows (F1, F6, F7).

Add a separate, self-declared **node profile** that every subsystem's placement defaults read (F3, F4, F5). Each subsystem then has a default that is a function of (relationship, profile) and an override per item: per drawer for sync and blobs, per processor, per pool.

## Open questions

1. Is the profile declared by the node about itself, or assigned by the user from another node? Self-declared is simpler, and a phone knows it is on battery.
2. Is the profile synced configuration (so sisters can plan around each other), or only announced at connection time?
3. Is recovery (F8) a third relationship, or a flag on mother?
4. Does a mother ever get `read`? For example, a user's own home server that runs processors would be both a mother and a sister. With two axes, that's simply "sister, profile always-on", and mother stays ciphertext-only.
5. Precedent to check before naming: Syncthing (per-folder device sharing plus "introducer"), Tailscale (tags and ACLs over device identity), Nextcloud and Resilio selective sync, and Kubernetes (node labels and taints as profile, with scheduling as placement defaults).
