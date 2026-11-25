# ADR 056: Node relationships, drawer roles and the default topology

**Status:** Accepted direction (operator, 2026-11-25). The defaults in §5 and the open questions at the end are
still to be confirmed.

**Amends:** [ADR 038](038-node-orientations-and-the-relay-alpha.md) §2. Its same-owner "sister" becomes **twin**,
and "mother" keeps its meaning. It also settles [ADR 019](019-cabinet-drawers-and-selective-mirroring.md)
§"Home and provider hints", which this ADR turns from a deferred design into the drawer-home role.

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
| K2 | **Offload (write-to-safe)** | time from a local write until the bytes sit on at least one always-on node that others pull from | an eager connection to mothers and drawer homes |
| K3 | **Availability** | whether a reader can fetch a drawer's latest state while every one of its writers' devices is asleep | every drawer has an always-on home |
| K4 | **Connection cost** | eager connections at boot and in steady state; time to first update when a non-eager drawer opens (cold open) | few eager connections, lazy dialing |
| K5 | **Exposure** | who learns that a node exists, its address, and which drawers it is interested in; who can reach it inbound | few contacts, no unsolicited inbound |
| K6 | **Resource fit** | storage, bandwidth and battery spent per node profile (phone, laptop, always-on server) | placement by profile |

K1-K3 trade against K4-K5. The default keeps the eager set small and owner-chosen (K4, K5). It buys K1-K3 back
through always-on nodes: mothers and drawer homes. Every e2e scenario for the topology reports the indicators it
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
  - By default it gets `relay` on every drawer the owner creates, and is a drawer home of each.
  - An owner may have several mothers. A node dials them in the owner's order and falls back to the next one
    when a mother is unreachable.

### 4. Drawer roles

These are per drawer, recorded on the drawer itself. They are separate from the access level, but constrained
by it.
- **Drawer home:** where the drawer's members are expected to pull it from and push it to.
  - It must hold at least `relay`.
  - A drawer created on my node gets my mothers as its drawer homes, published on the drawer descriptor
    (ADR 019's `DrawerHome { nodeId, addressHints[] }`).
  - Someone else's drawer has their mothers as its drawer homes.
  - A drawer names a small number of homes, 1 to 4: the list is a dialing set, not a replica census.
- **Pin:** a node that keeps the drawer for someone without being an advertised home.
  - Example: my mother keeping a drawer someone shared with me.
  - It needs `relay`, granted by whoever may delegate in that drawer.
  - It's my choice; a pin is not published on the drawer, and other members never dial it for that drawer.
- **Member:** every node with access.
  - Membership alone never makes one node dial another.

On a grant screen, the role is a field beside the access level ("give this node `relay`, and make it a drawer
home of this drawer"). The constraints:
- a drawer home needs at least `relay`;
- a twin must be a node of the same owner, i.e. in the owner's context group;
- a mother is a drawer home of new drawers by default.

### 5. The default topology

Dialing out:

| Moment | Dial out to | Why |
|---|---|---|
| Boot | my mothers, then my twins | the whole eager set: bounded and owner-chosen (K2, K4, K5) |
| A drawer becomes active (opened in a UI, or a processor, routine or micro subscribes) | that drawer's homes | lazy; kept while active, then for a linger period (K1, K4) |
| A drawer marked keep-warm | its drawer homes, at boot | an opt-in per drawer, for drawers I follow while they're closed |
| A local write | the drawer's homes, plus my mothers if they hold the drawer | over open connections, dialing a drawer home if none is open (K2) |
| Twins | each other, opportunistically | configuration and shared drawers; mothers cover them while they sleep |

Drawers often share homes (the same owner's mothers), so dialing is per home, not per drawer. One connection to a
home serves every active drawer it is home to. A node caps its lazy connections globally and evicts the
least recently active home first.

Inbound:

| Inbound from | Accepted |
|---|---|
| a twin | always |
| my mother | never initiated by it: it answers my dial |
| a member of a drawer I am a drawer home of | yes, for that drawer (what a mother or relay does) |
| a holder of a ticket I issued (an invite ticket) | yes, for that ticket's scope |
| anyone else | no reply at all, iroh pings included (F11) |

### 6. Vocabulary choices and prior art

- **Twin and mother** have no established meaning in other systems.
  - The nearest terms are Signal's primary and linked devices, which imply a rank that twins don't have.
  - For the mother's role: Tailscale's home DERP server, Secure Scuttlebutt's pubs and rooms, DIDComm's
    mediator, and the local-first essay's cloud peers.
- **Drawer home** is always written with "drawer".
  - Matrix's homeserver and AT Protocol's PDS ("your home in the cloud") are both the host of an *account*, so
    "home" alone would mislead.
  - The closest equivalent is Nostr NIP-65's write relays: where readers fetch an author from. We don't borrow
    "relay", because it's already a keyhive access level.
- **Pin** matches IPFS's pinning: keeping content from garbage collection. A pin for someone else is IPFS's
  pinning service, and a ciphertext-only one is Holepunch's blind peer.
- **Member** is keyhive's own sense.
- **Taken from prior art:**
  - bounded dialing sets: NIP-65's recommendation of 2-4 relays per list, and gossip clients' minimum-coverage
    selection, which become the 1-4 drawer homes and the global cap;
  - falling back across mothers, as clients do across DERP regions.

## Consequences

- ADR 038's "sister" becomes "twin" in code, docs and UI. Its "add a mother node" consequence stands, and now
  also publishes the mother as a drawer home of new drawers.
- The drawer descriptor gains published drawer homes (ADR 019's `DrawerHome`). A node's pins and keep-warm flags
  live in its own node policy (ADR 019 §"Per-node drawer policy"), not on the drawer.
- The peer policy (ADR 033's embedder hook, `big_node/peers/policy.rs`) is rewritten around §5:
  - an eager set of mothers and twins;
  - lazy, capped connections to drawer homes;
  - the inbound table, which also closes F11's open requirement (no reply to an unexpected peer).
- The relay alpha (ADR 038 §3) becomes a node whose profile is always-on and which is a mother for its users and
  a drawer home for their drawers. It keeps accepting members of the drawers it is home to.
- Invite tickets (identity-drawer-grants.md) are the one path by which a stranger may reach a node.
- A mother's own protocol needs its own spec, as DIDComm leaves mediator coordination to a separate protocol:
  - what a mother holds;
  - for how long after a drawer drops it (ADR 038 open question 4);
  - what happens when it is removed (question 3).

## Open questions

1. Which two indicators dominate the defaults? The proposal says K2 and K4. Are there target numbers?
2. Is "sister" retired, or kept for some other relationship?
3. The default linger period after a drawer goes inactive, and the global cap on lazy connections to drawer homes.
4. Keep-warm: is it set per drawer, or per class of drawer (e.g. every drawer shared by a given person)?
5. Is "members dial drawer homes, drawer homes never dial members" a hard rule? Or may a drawer home wake a sleeping
   member, e.g. through a mobile push channel?
6. Does adding a mother backfill it as a drawer home of existing drawers, or only new ones (ADR 038 open
   question 2)? Republishing drawer homes on old drawers changes what their members dial.
7. Is a deny list needed beside the allow-only inbound rule (Matrix applies server ACLs to every inbound request),
   for drawer homes or members a node no longer trusts?
