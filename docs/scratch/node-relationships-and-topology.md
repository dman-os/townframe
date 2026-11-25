# Node relationships and the default topology

- Status: proposal for discussion; nothing decided beyond what the operator said on 2026-11-25
- Owner: nodules lane
- Updated: 2026-11-25
- Scope: the vocabulary for how a node relates to other nodes (overall, and per drawer), and the default
  network topology that follows from it: whom a node dials at boot, when a drawer opens, and whom it lets in.
- Builds on: ADR 038 (orientations, relay alpha), `p2p-requirements.md` (forcing functions F1-F11),
  ADR 019 §"Home and provider hints", `identity-drawer-grants.md` (#349).

## What the operator said (2026-11-25)

- Relationships and profiles are separate axes (accepted).
- "Mother" stays. Today's "sister" (another node of the same owner, learning its configuration) may be better
  called a **twin**.
- Node-level words are not enough: a drawer from someone else lives on *their* relay, which is not my mother.
  I still want a relay-like relationship to it, so drawers need their own relationship vocabulary.
  - I can also put that drawer on my mother, so my mother pins it for me, but that is a choice, not the default.
- The vocabulary decides who a node talks to:
  - At boot: my mothers (to get what was pushed there) and my twins.
  - On opening a drawer: the drawer's mothers or homes, never every reader of the drawer, and not eagerly for
    drawers I'm not working with.
- Inbound: twins may contact me any time. Mothers never contact me; I contact them, and they pull from me over
  that connection (an upload).
- Per drawer, a node is granted a **role** (mother, ...) separately from its access level, though the two
  are related.
- Order of work: first the performance indicators, then the vocabulary and the default topology derived
  from them.

## 1. Performance indicators (proposed)

What the default topology is judged by. Each one is measurable in an e2e scenario.

| # | Indicator | Measured as | Pulls toward |
|---|---|---|---|
| K1 | **Freshness** | time from a write on one node to its visibility on another interested node (p50/p95) | more and earlier connections |
| K2 | **Offload (write-to-safe)** | time from a local write until the bytes sit on at least one always-on node that others pull from (my mother or the drawer's home) | an eager connection to mothers/homes |
| K3 | **Availability** | can a reader fetch the latest state of a drawer while every one of the writer's devices is asleep | every drawer has a home that is always on |
| K4 | **Connection cost** | eager connections at boot and in steady state; time to first update when a non-eager drawer is opened (cold open) | fewer eager connections, lazy dialing |
| K5 | **Exposure** | who learns that my node exists, its address, and which drawers it is interested in; who can reach it inbound | fewer contacts, no unsolicited inbound |
| K6 | **Resource fit** | storage, bandwidth and battery per node profile (phone vs laptop vs mother) | placement by profile |

K1/K2/K3 trade against K4/K5. The topology below fixes the eager set small (K4, K5) and buys K1-K3 back
through mothers and homes, which are always on.

## 2. Vocabulary (proposed)

Two layers. Neither replaces keyhive's access level (`relay`, `read`, `write`, `admin`), which stays the only
thing that decides what a node may *hold or read*. Roles decide *who talks to whom, and who is expected to keep
what*.

### Node relationships (between nodes, carried by the owner's root/configuration drawer)

- **twin**: another node of the same owner. It learns my configuration (F6, F7), holds plaintext, and may dial
  me at any time. This is today's "sister" (ADR 038 §2).
- **mother**: an always-on node that I back up to and rendezvous through (F2, F9). I dial it and it never dials
  me. By default it gets `relay` on drawers I create, and it is their home.

### Drawer roles (per drawer, recorded on the drawer, ADR 019's `DrawerHome`)

- **home**: where members are expected to pull the drawer from, and push to. A home needs at least `relay`
  access. A drawer created on my node gets my mothers as its homes. Someone else's drawer has its owner's
  mothers as homes.
- **pin** (or keeper): a node that keeps the drawer for someone without being one of its homes, e.g. my mother
  keeping a drawer someone shared with me. It's my choice; it takes `relay` for my mother, granted by whoever
  may delegate in that drawer.
- **member**: everyone else with access. Membership alone never makes a node dial another.

Open: whether "home" should be spelled "the drawer's mother", which reuses the operator's word at drawer
level. "Home" is already in ADR 019 and keeps "mother" for the node-level relationship.

## 3. Default topology (proposed)

| Moment | Dial out to | Notes |
|---|---|---|
| Boot | my mothers, my twins | the only eager set; bounded and owner-chosen (K4, K5) |
| A drawer becomes active (opened in a UI, a processor or routine subscribes) | that drawer's homes | lazy; kept while active plus a linger timeout |
| Per-drawer "keep warm" | that drawer's homes, at boot | an opt-in per drawer, for drawers I care about even when closed |
| A local write | my mothers (if they hold the drawer) and the drawer's homes | over existing connections, dialing a home if none is open (K2) |
| Twins | each other, opportunistically | config and shared drawers; the mother covers them while they sleep |

| Inbound from | Accepted? |
|---|---|
| a twin | always |
| my mother | never initiates; it answers my dial, and pulls my uploads over that connection |
| a node for which I am a drawer's home | yes, for that drawer (this is what a relay or mother does) |
| anyone else | only with a ticket I issued (an invite ticket, #349), otherwise no reply at all, iroh ping included (F11) |

## 4. How grants meet roles

- Granting is still keyhive's `InvitePrincipal` with an access level.
- A role is a second, separate field on the same grant screen: "give this node `relay` and make it a home of
  this drawer".
- Constraints: home ⇒ at least `relay`; twin ⇒ same owner (the owner's context group, #349); a mother is a home
  of new drawers by default, and backfilling existing ones is ADR 038's open question 2.

## Questions for the operator

1. Are K1-K6 the right indicators? Which two matter most for the default (I'd say K2 offload and K4 connection
   cost)? Are there target numbers?
2. Is "twin" the same-owner relationship? If so, what is "sister" now: retired, or reserved for something else?
3. Drawer level: "home" (ADR 019's word) or "drawer mother"?
4. Is "pin on my mother" a role of its own, or just "my mother is also a home of that drawer, for me"?
5. Linger and keep-warm: a default linger time, and is keep-warm per drawer or per class of drawer?
6. Does a home ever dial its members (a push to sleeping members over a mobile push channel, say), or is
   "members always dial homes" a hard rule?
