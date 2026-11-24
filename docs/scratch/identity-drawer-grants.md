# Granting a drawer to a person, not just their phone

Status: proposal for review, nothing decided
Owner: nodules lane
Updated: 2026-11-24
Builds on: `identity-layers.md` (branch `stack/node-90-identity-layers`), whose delineation the operator accepted: actors
are runtime principals bound to a device, identities are DIDs (data, cross-node), and link records join them. Keyhive
groups are a separate membership and recovery mechanism.

## The question

"How do I grant a drawer to a person, not just to their phone?" Today you can only name a node. `day drawers add-member
<drawer> <peer> --access ...` (`src/day_cli/cmds/drawers.rs:236`) sends `InvitePrincipal` with `Principal::Peer`, a
node key, or `Principal::Group`, a keyhive group id (`src/big_cabinet_core/drawer/protocol.rs:395`). The node resolves
the peer through the keyhive agent it learned from a live connection, and refuses a peer it has not met with
`UnknownPeer` (`src/big_cabinet/drawer/principal.rs:12-27`).

Answering "a person" takes three steps: say what a person is at this layer, say how you get from that to keyhive
principals, and say what the answer leaks.

## 1. What an identity is here

An identity is a DID, and the design must not care which method. Four are in view:

| Method | Where its data lives | What we assume of it |
|---|---|---|
| `did:key` | Nowhere; the key is the identifier | Nothing published. Everything about it arrives out of band. |
| `did:web` | A file on a domain the person controls | Can carry service entries or a well-known file. |
| `did:plc` | A public log plus the person's PDS repo | Records in the PDS repo, service entries in the DID document. |
| Our own (later) | Keyhive documents | Not designed. The interface below must fit it. |

Nothing below needs more than this: **a DID can verify a signature, and a resolver can return what the identity chooses
to say about itself to a given audience.**

### The resolution interface (a proposal, not an implementation)

The lower layer asks one question: given an identity, which keyhive principals act for it, as far as this asker is
entitled to know?

```text
resolve(did, context?) -> Vec<Endpoint>

Endpoint =
  | Node   { keyhive_individual, contact_card }       one device, grantable offline
  | Group  { keyhive_group, sync_hint: Node }         a set of devices, with a node that serves its ops
  | Recovery { keyhive_individual }                    ADR 006, never granted content
each carries: signed by the identity, optional expiry, optional purpose
```

An endpoint reaches the asker by one of three channels, from most to least public:

1. **A public record.** For `did:plc`, a record in the PDS repo (a collection of ours, with one rkey per endpoint).
   For `did:web`, a well-known file. Anyone can read it.
2. **An introduction.** The person hands the asker an endpoint directly: a QR code, a pasted ticket, a message. This
   is how a relationship starts, and it is the only channel for `did:key`. It can be narrow because it reaches one party.
3. **A record inside a shared drawer.** Once two parties share any drawer, the identity can write a signed endpoint
   datum there, visible to that drawer's members only.

The resolver is therefore a lookup that tries channels in order and verifies signatures, not a directory. The result
never grants anything: ADR 043 §7 holds ("identity is not authority"). The grant is a keyhive membership on the
returned principals. The identity only tells you which principals to put it on.

## 2. Is the link from identity to devices always a keyhive group?

No. The standard configuration is a group, but four shapes are possible:

| Shape | What the link is | Fits when | Costs |
|---|---|---|---|
| **A. Keyhive group** | One group per person or per context, devices as members. The identity publishes the group id. | A person with several devices and ongoing sharing. The usual case. | Group ops are visible to everyone who evaluates the drawer's graph. Granting needs the group's ops and every member's prekeys. |
| **B. A list of individuals** | The identity publishes several `Node` endpoints. The granter adds each one. | Two devices, a short-lived share, or a peer whose node cannot hold a group. | The granter must re-resolve to learn about new devices. A lost device stays a member until each granter removes it. |
| **C. A signed link record only** | The identity-layers link record (actor A on node N acts for me) and nothing in keyhive. | Authorship and display, which is its job. | It grants nothing. To share, you still pick principals by shape A or B. |
| **D. One home node** | The identity names one node (often a relay or a server) that all its devices sync through. The granter adds only that node. | Someone who wants one stable address and no device list at all. | The home node reads what it is granted (unless it is `Relay` level). It is a single point of trust. |

A group wins on maintenance: add a device to the group and every drawer granted to the group reaches it, with no action
by the granter. Keyhive's design says as much for compromise: "a user group delegating to per-device keys limits the
blast radius to one device, which the user group can revoke" (`keyhive/design/threat_model.md`, A4). The other shapes
win on what they reveal, as the next section shows. They also compose: a person can run a group among their own
devices and publish only one `Node` endpoint from it.

## 3. Metadata privacy: what each party learns

The operator does not want every device on the PDS. Start from what keyhive itself exposes, because that is the floor:

- A `Relay` holder "sees operation hashes, sizes, timing, and the membership graph" (threat model A1, and T8:
  "Relays must see membership to evaluate capabilities").
- Every reader's individual is a leaf in the document's BeeKEM tree (`convergent_capabilities.md:92`). Co-members of a
  drawer see the individuals behind a granted group, not only the group id.

So **a group hides its device set from outsiders, not from the drawer it is granted into.** Anyone who can read that
drawer learns that group's devices. The privacy unit is the drawer's audience, not the identity.

Now the options, by who learns what. "Context group" means one group per relationship or context (work, family, a
relay), each with its own member list.

| Party | One global group, published | One global group, introduced only | Per-context groups | A single `Node` endpoint per context |
|---|---|---|---|---|
| PDS and anyone reading it | All devices, if the id and cards are public | Nothing | Nothing | One node, only if published |
| A relay holding the drawer | That group's devices | That group's devices | The context group's devices only | One node |
| Another person granted a drawer | All devices | All devices | Only the devices in the group they were given | One device |
| Granter | All devices | All devices | Only that context's devices | One device |

The table shows the trade. **Per-context groups let the person decide, relationship by relationship, how many devices
a party sees.** The relay learns the devices of the contexts it serves, and no more.

One leak survives, and the operator should know about it. A node key is a keyhive individual, and the same node key
appears in every group it belongs to. If a laptop is in both the "work" and "family" groups, parties who compare notes
can see that both groups contain that individual. Per-context groups hide *which other devices*, not that two contexts
share a device. Closing that gap means a separate keyhive individual per context on each device. `big_repo` has one local
agent today (`local_keyhive_contact_card()` returns a single card, `src/big_repo/lib.rs:646`), so that is a new
feature, not a configuration. I recommend not building it yet (question 3).

ATSMS treats this case thinly: see section 6.

## 4. Offline granting: contact cards and prekeys

Keyhive was built for adding someone who is not online. An individual publishes prekeys, public keys that an inviter
uses as the first key for the invitee's slot in a document's BeeKEM tree. "The invited `Individual` is offline, but
needs to be added to the encryption tree for a particular `Document`. They publish a set of public keys in advance"
(`keyhive_core/src/principal/individual.rs:46-59`). A **contact card** is one signed prekey operation naming the
individual (`contact_card.rs:12`, a single `KeyOp`). `Keyhive::receive_contact_card` registers the individual from it
(`keyhive.rs:515`), and `add_member` then picks a prekey per document deterministically and seats the grantee
(`peer.rs:62`, `keyhive.rs:716`). When the grantee next comes online, the prekey janitor rotates the consumed prekey out
and refills the pool to a floor of 8 (`src/big_repo/runtime2/prekey_janitor.rs`).

What the grantee must have published beforehand:

- **A contact card** the granter can get: by channel 1, 2 or 3 above. One card is one prekey, enough for one device.
- **Prekeys that have not run out.** The card carries one. A grant touches several documents (a drawer's docs group
  and its meta groups, `invite_principal` in `principal.rs:146`), so a card that is a few rotations stale can leave
  the granter with no key (`MissingPrekeys`) or with a key another inviter already consumed.
- **For a group, the keys of every current member.** `pick_individual_prekeys` walks the group and needs a prekey for each
  individual in it (`agent.rs:171`). A group grant therefore needs the group's membership ops and a card or prekey ops
  per device, not one card. This is why a group endpoint carries a `sync_hint` node, and why an introduction bundle for a
  group is the group id plus the current cards. Whether keyhive can export a group's ops as a self-contained bundle,
  rather than syncing them, is not yet checked.

A gap in our own code: `BigRepo::receive_keyhive_contact_card` exists and the e2e tests use it
(`src/big_cabinet/drawer/e2e/peer.rs:61`), but no node method or CLI command exposes it. Today the only way to learn a
peer is to connect to it (`day peers add <ticket>`), so "offline" grants cannot happen from the CLI. The first build is
a `day peers add-card <card>` and the matching node method.

## 5. The concrete flows

Three flows, from the widest to the narrowest. The cartridge flow is for contrast: it is the existing mechanism, and it
touches no identity.

### 5.1 Grant a drawer to a person (all devices, or a context group)

1. **Resolve.** `day drawers grant <drawer> <did> --access read [--context work]`. The node calls `resolve(did, context)`.
   It returns a `Group` endpoint (or several `Node` endpoints) from whichever channel the person used with you.
2. **Prepare.** The node registers the individuals from their cards, syncs the group's ops through the `sync_hint`, and
   checks that it holds a prekey per member. If one is missing it says which device and what to ask for.
3. **Grant.** `InvitePrincipal { principal: Group(..), access }`, unchanged. Keyhive seats every current member in the
   drawer's trees from their prekeys. No connection to the person is needed.
4. **Later.** A device added to the group afterwards is seated by any reader who holds the tree. Keyhive added this so a
   group's new member is not stranded (`keyhive.rs`, the "seat in `doc_id`'s key tree every reader that the tree is
   missing" function).

The granter sees: a group id with N leaves, the DID that gave it to them (they hold the signed endpoint), and the
drawer's membership as keyhive reports it. The grantee's devices see: when any member device next syncs, the drawer
appears and its content syncs, subject to their own node's policy. An open question is whether it arrives silently
(question 7).

Revoking is `RevokePrincipal` on the group, or the person removes a device from their own group, which ends that
device's access everywhere the group is granted.

### 5.2 Grant a drawer to one device

Today: `day drawers add-member <drawer> <node key> --access read`, which needs the peer to have been met. Proposed:
the same command, taking a contact card instead of a met peer, via the new registration step in section 4. The
granter sees one individual. The grantee sees one device get it. No identity is involved, which is the point: this is
the case where the person wants a drawer on exactly one device and nothing else learns about the rest.

### 5.3 Grant a drawer to a cartridge on your own node (ADR 049, for contrast)

`GrantDrawer` writes a standing grant to a local actor in the drawer service's ledger (`src/big_cabinet/drawer/grant.rs`).
The person consents in the cartridge's terms (ADR 049, opening decisions 6 and 7: the person faces the cartridge, and one answer
satisfies every permbind). The runtime mints a capability for the cartridge's actor from that grant.

| | Person or device (5.1, 5.2) | Cartridge (5.3) |
|---|---|---|
| Authority comes from | A keyhive delegation, which cryptography enforces on every node | A node-local grant row, which the node enforces |
| Principal | A keyhive individual or group, by key | A local actor, by path |
| Who else learns | Drawer co-members and any relay | Nobody, it is local |
| Revoked by | Keyhive revocation, propagated | Deleting the grant, effective on the next re-mint |
| Identity involved | Only to look up principals | None |

Note that 5.1 and 5.2 grant to a **node**. What the grantee's *actors* may then do with the drawer (a cartridge there
reading it) is a second, local grant on their side, by 5.3. A person's drawer membership never gives a cartridge on
their node access by itself.

## 6. Prior art: ATSMS

**Found.** ATSMS is "AT Secure Messaging Service", an open proposal for end-to-end encrypted group messaging on AT
Protocol identities, by 3NUM Labs. Sources: [the repository](https://github.com/3numlabs/atsms),
[the announcement](https://www.3num.co/3num-blog-2/introducing-atsms) (Chris Boscolo, September 2025), and these
specs in the repository: `spec/identity-devices.md`, `spec/sealed-sender.md`, `spec/atsms-integration.md`. Honest
limits: I read these through a fetch tool that summarises pages, not as raw text, and I read three specs, not all
twelve. Its own README says it has had no external cryptographic review, and the specs are drafts. Before relying on a
claim below, check it against `spec/identity-devices.md`.

What it says, as I read it:

- **Linkage is public, per device.** "A device is delegated by publishing its endpoint certificate as a record in the
  DID's repo." Each device has an `at.atsms.x509` record and an `at.atsms.prekey` record, keyed by the device
  fingerprint, in the person's PDS repo. A `at.atsms.inbox` record names the DID's delivery endpoints.
- **No device-list record.** The specs do not say how a sender enumerates a person's devices.
- **Devices manage each other.** Devices of one DID may add or remove each other regardless of role. Loss reduces to a
  remove by another device. If no device remains, the spec does not say how to recover (our ADR 006 recovery principal
  is that answer for us).
- **Core is BeeKEM.** It ported BeeKEM, the same group-key scheme keyhive uses, to TypeScript and checked it against
  the keyhive Rust crate. It is not built on keyhive's membership graph or capabilities.
- **Metadata privacy is about the wire, not devices.** A sealed-sender envelope hides the sender from the mailbox
  provider, and sizes are bucketed. The spec lists as unaddressed "per-context or per-contact device privacy" and what
  other group members learn about a device. It also admits that, under its first-version policy, an endpoint operator
  can link one device's traffic across groups.

So ATSMS is what the operator does not want for devices: publish every device. It is useful to us on two points. It
shows that publishing per-device records in the PDS is a workable shape for sender discovery, and its sealed-sender
work is a model if a relay should not learn which person is behind a connection. It does not answer the per-context
question. I did not find where it "figures this out" beyond that. If the operator had a specific passage in mind,
please point me at it (question 9).

Not read, so not cited for content: Signal's Sesame device-list design, MLS, Matrix cross-signing, Bluesky's own DM
design, and a separate atproto instant-messaging proposal using MLS and Iroh
([discussion](https://github.com/blacksky-algorithms/rsky/discussions/70), surfaced by search only). They are leads
for a second pass.

## 7. Recommended shape

- A person's standard link is **a keyhive group per context**, and nothing about devices is public by default.
- The PDS carries at most **one introduction endpoint** (one node's contact card, or a relay-hosted introduction
  address). Everything else travels by introduction or inside a shared drawer.
- The resolver is `resolve(did, context)` returning signed endpoints, and works from `did:key` up.
- Granting is unchanged at the keyhive level (`InvitePrincipal`), and gains a resolution step before it and a
  contact-card registration path for offline grants.
- A cartridge grant stays local and separate.

## Questions for the operator

1. **Is a keyhive group per context the standard link?** Recommend yes, with shapes B and D permitted. A group makes
   device changes propagate without the granter, and per-context groups give the privacy asked for.
2. **What goes on the PDS?** Recommend one introduction endpoint per identity, and no device list. Confirm that one node
   being public is acceptable, or say it should be none (then discovery is by introduction only).
3. **Do we accept that a device shared by two contexts links them?** Recommend yes for now. A key per context per device
   is a large feature (a second local keyhive agent); revisit if linkage across contexts becomes a real threat.
4. **Expose contact-card registration (`day peers add-card`) as the first build?** Recommend yes: it is a small, useful
   gap and it unblocks offline device grants before any identity work.
5. **What is an introduction bundle for a group?** Recommend the group id plus the current cards plus a `sync_hint`,
   pending a check that keyhive can export a group's ops without a live sync.
6. **Who is Admin of a person's context group?** Recommend the person's devices at Edit or Admin among themselves, and
   the recovery principal (ADR 006) at Admin on the group, never on the content.
7. **Should a granted drawer need the grantee's consent before it syncs?** Recommend yes, an offer the person accepts.
   The granter would otherwise push data onto devices that have not agreed to hold it. I have not checked what happens
   today when a node becomes a keyhive member of a drawer it never asked for.
8. **Where does `resolve` live?** Recommend a node-side service in the lower layer, `did:key` first, with the PDS and
   `did:web` channels added when the AT Protocol bridge is built.
9. **Which part of ATSMS did you have in mind?** I found and read the three specs above. If you recall a specific
   proposal (the repository has a `proposals/` folder I did not open), name it and I will read it. Recommend a short
   second pass on Signal's Sesame and Matrix cross-signing, since both concern device lists with privacy.
