# ADR 056: Node relationships, drawer roles and the default topology

**Status:** Accepted direction (operator, 2026-11-25): mother and twin between nodes; member and anchor per drawer. The defaults in §5 and the open questions at the end are
still to be confirmed.

**Amends:** [ADR 038](038-node-orientations-and-the-relay-alpha.md) §2. Its same-owner "sister" becomes **twin**,
and "mother" keeps its meaning. It also settles [ADR 019](019-cabinet-drawers-and-selective-mirroring.md)
§"Home and provider hints", which this ADR turns from a deferred design into the anchor role.

**Related:**
- [P2P requirements and forcing functions](../scratch/p2p-requirements.md) (F1-F11, cited below)
- [Granting a drawer to a person](../scratch/identity-drawer-grants.md) (invite tickets, context groups)
- [ADR 033: what a node is](033-what-a-node-is.md) (the embedder's peer policy, which this ADR fills in)

## Context

ADR 038 gave a user's nodes two orientations: mothers (always-on backup) and sisters (the user's other devices).
That vocabulary only covers the user's own nodes and the user's own drawers. It says nothing about a drawer
someone else shared with me. That drawer lives on its owner's always-on nodes, not on my mother. I still want a
relay-like relationship to it, and I may optionally keep a copy on my mother too.

The vocabulary also decides the network's behaviour, not just labels:
- whom a node dials at boot;
- whom it dials when a drawer is in use;
- whom it lets in.

Dialing every reader of every drawer is wrong for battery, for connection counts and for privacy. So the operator
asked for the performance indicators first, and for the vocabulary and the default topology to be derived from them.

Access levels are not enough on their own. keyhive's `relay`, `read`, `write` and `admin` say what a node may
hold or read (ADR 038 §1). They say nothing about who is expected to be always on, whom to pull from, or whom to
dial.

## Decision

### 1. The topology is judged by six indicators

| # | Indicator | Measured as | Pulls toward |
|---|---|---|---|
| K1 | **Freshness** | time from a write on one node to its visibility on another interested node (p50/p95) | more, earlier connections |
| K2 | **Offload (write-to-safe)** | time from a local write until the bytes sit on at least one always-on node that others pull from | an eager connection to mothers and anchors |
| K3 | **Availability** | whether a reader can fetch a drawer's latest state while every one of its writers' devices is asleep | every drawer has an always-on anchor |
| K4 | **Connection cost** | eager connections at boot and in steady state; time to first update when a non-eager drawer opens (cold open) | few eager connections, lazy dialing |
| K5 | **Exposure** | who learns that a node exists, its address, and which drawers it is interested in; who can reach it inbound | few contacts, no unsolicited inbound |
| K6 | **Resource fit** | storage, bandwidth and battery spent per node profile (phone, laptop, always-on server) | placement by profile |

K1-K3 trade against K4-K5. The default keeps the eager set small and owner-chosen (K4, K5). It buys K1-K3 back
through always-on nodes: mothers and anchors. Every e2e scenario for the topology reports the indicators it
moves.

### 2. Relationships and profiles are separate axes

Two separate questions:
- **Relationship:** who a node is to me, and how far I trust it. It decides eligibility to hold or read data and
  compiles into keyhive membership.
- **Profile:** what a node can afford (always on, storage class, compute class, on battery, reachable from
  outside). It decides defaults among the eligible nodes.

A node declares its own profile (p2p-requirements, open question 1). Placement defaults read both axes:
- the blob pins of F3;
- processors (F4);
- executor pools (F5).

### 3. Node relationships

These hold between nodes, and they live in the owner's root (configuration) drawer, which the owner's twins
share (F6).
- **Twin:** another node of the same owner.
  - It learns the owner's configuration (F6, F7) and holds plaintext.
  - It may dial its twins at any time.
  - This is ADR 038's sister, renamed: two twins are peers of equal standing, and neither is primary.
- **Mother:** an always-on node the owner backs up to and rendezvous through (F2, F9).
  - The owner's nodes dial it, and it never dials them. It pulls their uploads over the connection they opened.
  - By default it gets `relay` on every drawer the owner creates, and is an anchor of each.
  - An owner may have several mothers. A node dials them in the owner's order and falls back to the next one
    when a mother is unreachable.

### 4. Drawer roles

Every member that holds a drawer hosts it in the plain sense. That's keyhive membership, and it says nothing about
where to look. A drawer adds one published claim: where it can be expected to be found.
- **Member:** every node with access (keyhive's `relay`, `read`, `write` or `admin`).
  - Membership alone never makes one node dial another.
  - Keeping someone else's drawer on my mother is plain membership: my mother is a `relay` member, granted by
    whoever may delegate in that drawer. It's not a separate role.
- **Anchor:** a member named by the drawer itself as where its members can expect to find it, pull it from, and
  push it to.
  - Anchors are listed in the drawer's own metadata as an `anchors` datum: ADR 019's
    `DrawerHome { nodeId, addressHints[] }`, renamed `DrawerAnchor`, one per anchor. They are not inferred from keyhive.
  - An anchor must be a member with at least `relay`.
  - A drawer created on my node lists my mothers as its anchors. Someone else's drawer lists their mothers.
  - A drawer names a small number of anchors, 1 to 4: the list is a dialing set, not a replica census.

The drawer panel then reads in one vocabulary, per node with access:
- the node's relationship to me (my mother, my twin, or none);
- its access level;
- whether the drawer lists it as an anchor.

Granting anchor is a field beside the access level ("give this node `relay`, and list it as an anchor of this
drawer"). The constraints:
- an anchor needs at least `relay`;
- a twin must be a node of the same owner, i.e. in the owner's context group;
- a mother is listed as an anchor of new drawers by default.

### 5. The default topology

Dialing out:

| Moment | Dial out to | Why |
|---|---|---|
| Boot | my mothers, then my twins | the whole eager set: bounded and owner-chosen (K2, K4, K5) |
| A drawer becomes active (opened in a UI, or a processor, routine or micro subscribes) | that drawer's anchors | lazy; kept while active, then for a linger period (K1, K4) |
| A drawer marked keep-warm | its anchors, at boot | an opt-in per drawer, for drawers I follow while they're closed |
| A local write | the drawer's anchors, plus my mothers if they hold the drawer | over open connections, dialing an anchor if none is open (K2) |
| Twins | each other, opportunistically | configuration and shared drawers; mothers cover them while they sleep |

Drawers often share anchors (the same owner's mothers), so dialing is per anchor, not per drawer. One connection to an
anchor serves every active drawer it anchors. A node caps its lazy connections globally and evicts the
least recently active anchor first.

Inbound:

| Inbound from | Accepted |
|---|---|
| a twin | always |
| my mother | never initiated by it: it answers my dial |
| a member of a drawer I anchor | yes, for that drawer (what a mother or relay does) |
| a holder of a ticket I issued (an invite ticket) | yes, for that ticket's scope |
| anyone else | no reply at all, iroh pings included (F11) |

### 6. Vocabulary choices and prior art

- **Twin and mother** have no established meaning in other systems.
  - The nearest terms are Signal's primary and linked devices, which imply a rank that twins don't have.
  - For the mother's role: Tailscale's home DERP server, Secure Scuttlebutt's pubs and rooms, DIDComm's
    mediator, and the local-first essay's cloud peers.
- **Anchor** (operator, 2026-11-25) means one specific thing: a member the drawer lists as where it can be
  expected to be found, called an anchor node in the UI. A member has the drawer; an anchor has it strongly, and says
  so.
  - Its familiar precedent is Hyperledger Fabric's anchor peers. Like a drawer's anchors, they are listed in the
    shared object's own configuration (the channel's) so that others can find it. The closest mechanism
    elsewhere is Nostr NIP-65's write relays: where readers fetch an author from.
  - "Home" (ADR 019's word) was rejected because it implies a single place and reads as *my* home. Account-level
    hosting, as a PDS provider would do it (Matrix's homeserver, AT Protocol's "your home in the cloud"), is a
    mother.
  - "Host" was rejected: every member holding the drawer hosts it, so it would not single anything out.
  - "Relay" was rejected: it is already a keyhive access level, and an anchor is not necessarily ciphertext-only.
  - "Seed" (BitTorrent seeders, Cosmos seed nodes) was rejected: it collides with the node's identity seed.
- **Pin** was dropped: keeping someone else's drawer for them (IPFS's pinning service, Holepunch's blind peer) is
  membership with `relay`, so it needs no word of its own.
- **Member** is keyhive's own sense.
- **Taken from prior art:**
  - bounded dialing sets: NIP-65's recommendation of 2-4 relays per list, and gossip clients' minimum-coverage
    selection, which become the 1-4 anchors and the global cap;
  - falling back across mothers, as clients do across DERP regions.

## Consequences

- ADR 038's "sister" becomes "twin" in code, docs and UI. Its "add a mother node" consequence stands, and now
  also publishes the mother as an anchor of new drawers.
- The drawer's metadata gains an `anchors` datum (ADR 019's `DrawerHome`, renamed `DrawerAnchor`). No such datum exists in code yet. A
  node's keep-warm flags live in its own node policy (ADR 019 §"Per-node drawer policy"), not on the drawer.
- The peer policy (ADR 033's embedder hook, `big_node/peers/policy.rs`) is rewritten around §5:
  - an eager set of mothers and twins;
  - lazy, capped connections to anchors;
  - the inbound table, which also closes F11's open requirement (no reply to an unexpected peer).
- The relay alpha (ADR 038 §3) becomes a node whose profile is always-on and which is a mother for its users and
  an anchor for their drawers. It keeps accepting members of the drawers it anchors.
- Invite tickets (identity-drawer-grants.md) are the one path by which a stranger may reach a node.
- A mother's own protocol needs its own spec, as DIDComm leaves mediator coordination to a separate protocol:
  - what a mother holds;
  - for how long after a drawer drops it (ADR 038 open question 4);
  - what happens when it is removed (question 3).

## Open questions

1. Which two indicators dominate the defaults? The proposal says K2 and K4. Are there target numbers?
2. ~~Is "sister" retired?~~ Retired for now (operator, 2026-11-25), and kept free for a relationship that may need it soon.
3. The default linger period after a drawer goes inactive, and the global cap on lazy connections to anchors.
4. Keep-warm: is it set per drawer, or per class of drawer (e.g. every drawer shared by a given person)?
5. Is "members dial anchors, anchors never dial members" a hard rule? Or may an anchor wake a sleeping
   member, e.g. through a mobile push channel?
6. Does adding a mother backfill it as an anchor of existing drawers, or only new ones (ADR 038 open
   question 2)? Republishing anchors on old drawers changes what their members dial.
7. Is a deny list needed beside the allow-only inbound rule (Matrix applies server ACLs to every inbound request),
   for anchors or members a node no longer trusts?
