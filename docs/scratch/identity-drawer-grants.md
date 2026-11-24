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

## Keyhive, concretely: what leaks and who must be told

The operator asked seven questions that the earlier sections answered from design docs. These answers are from source:
keyhive at `/run/media/asdf/p3N/repos/keyhive` (`keyhive_core/src/...`), the sync protocol in `../subduction`
(`subduction_keyhive/src/protocol.rs`), and our `src/big_repo`. "Not established" means I could not settle it from code.

**1. Is there a contact card for a group? What does a group's id reveal?**
No group card exists. A `ContactCard` wraps one `KeyOp`, a prekey operation signed by an individual
(`keyhive_core/src/contact_card.rs:12`), and `Keyhive::contact_card` returns the local individual's (`keyhive.rs`, the
accessor just above `receive_contact_card` at `:515`). A group is identified by a key and a delegation graph
(`principal/group.rs:68-86`: `id_or_indie`, `members`, `state`). The id alone reveals nothing, because it is a public key. Whoever
holds the group's *delegation ops* can enumerate its members: `Group::members()` lists every delegate, and
`Agent::pick_individual_prekeys` walks them down to individuals (`principal/agent.rs:171-205`). Nested groups flatten in
that walk, so nesting hides nothing. Holding the id without the ops is not enough to do anything: `Keyhive::add_member`
looks the agent up by id and fails if it is not held (`keyhive.rs:716-719`).

**2. To grant a group access to a document, does the granter learn every member device?**
Yes, and so does every later reader. At grant time `pick_individual_prekeys` returns a prekey for every individual
reachable through the group, and fails with `MissingPrekeys` if any is absent (`agent.rs:171-205`; `peer.rs:62`). Those
individuals become leaves of the document's CGKA tree through `add_cgka_members_from_prekeys` (`keyhive.rs:~700`, in
`add_member_with_manual_content`). Afterwards the server hands every reader the CGKA ops, the group's delegations and each reachable
member's prekey ops: `events_for_agent` is the union of `membership_ops_for_agent`, `reachable_prekey_ops_for_agent`
and `cgka_ops_reachable_by_agent` (`keyhive.rs:1546-1570`, `1758-1830`), and a reader reaches the document so it
receives all three. A device added to the group later is seated by any reader that holds the tree
(`reconcile_doc_readers`, `keyhive.rs:750-766`), so readers learn it as soon as they sync the group. The operator's
worry is justified: **a malicious or compromised granter learns every device in the group you hand them**, at the moment
of the grant, including devices that never open the drawer, and learns each device added to that group later. What they cannot learn is devices outside that group. So the grantee
chooses what to expose by choosing which group to present, which is the per-context design of section 3.

**3. Relays.**
A relay holding the drawer must hold the group's delegations too, or it cannot serve the group's devices. It computes
membership by walking the graph (`Membered::transitive_members`, `principal/membered.rs:101`), and a delegation
naming a group the relay has not heard of does not apply: it parks with `UnknownAgent`, which `is_missing_dependency`
reports as true (`keyhive.rs:2264-2268`, `:3878`). The relay also receives what any reachable agent receives:
membership ops, prekey ops and CGKA ops (the same functions as question 2, and keyhive's own threat model: a
`Relay` holder "sees ... the membership graph", `design/threat_model.md`, A1 and T8). Keyhive has no "ask the relay to
reveal" request. A granter who is a member simply receives the group's ops and members' prekey ops when it syncs,
because they are reachable from the drawer. In `big_repo`, the dispatcher sends only payload-free change hints
to peers whose visibility covers an event, and wakes *every* connected peer except the source for contact-card prekey
ops (`src/big_repo/runtime2/keyhive_dispatcher.rs:12-19`, the "broadcast-like audience"). The events themselves move in
`handle_sync_request`, which serves what `get_events_for_peer_pair` computes for that peer's identity
(`protocol.rs:895-960`, `:1415-1460`). A peer the server does not know gets `RequestContactCard` instead (`:918-927`).

**4. Must a delegation be broadcast? Can a device present the grant later?**
Not broadcast. Keyhive has no broadcast: an op reaches a peer only when that peer syncs and the server finds the op
reachable for it. So the delegation (drawer to group) can live only at the drawer's relay. Three things then limit the
operator's model of "devices become known when one asks":

- *Membership can arrive late.* A device that connects, becomes known through its contact card, and brings its group's
  ops lets the parked delegation apply. `handle_sync_ops` ingests events from the sender without a membership check
  that I could find (`protocol.rs:1210-1260`). Whether our relay policy adds one is ADR 005 §2 (refuse unknown peers),
  which is a design, and I did not trace it in code. Not established.
- *Decryption cannot arrive late without a reader.* A device must be a CGKA leaf to decrypt, and a leaf is added from
  its prekey by someone who holds the tree and reads the document. A `Relay`-only server holds no tree
  (`design/convergent_capabilities.md:92`: `Relay` is excluded from the tree). So the device receives nothing
  readable until the granter, or another reader, comes online and seats it (`reconcile_doc_readers`).
- *Someone has to know the group at grant time.* `add_member` needs the group held locally (question 1), and a group is
  built from a delegation head (`group.rs:~95`), so the granter holds at least the founding member. Keyhive does promote
  an individual to a group when a delegation names it as subject (`keyhive.rs`, `promote_individual_to_group`, called
  from `receive_delegation`), and I did not establish whether that allows a group with members the granter never sees.
  Not established.

So with keyhive as built, **the granter learns the grantee's devices when it seats them, not necessarily at grant time**,
and a device is never usable before some reader has seated it. "Present the grant when it asks" works for membership
and cannot work for decryption.

**5. Unlinkability across two accounts.**
Confirmed. A `Keyhive` holds exactly one local individual (`keyhive.rs:116`, `active: Arc<Mutex<Active<..>>>`), and
`BigRepo` exposes one card (`src/big_repo/lib.rs:646`). It also treats the iroh peer key and the keyhive individual as
the same 32 bytes (`lib.rs:658-660`, `KeyhivePeerId::from_bytes(peer_id.to_bytes32()?)`). A device in two accounts'
groups is therefore one individual in both, visible to every co-member of both drawers and to any relay that holds
both. Separate individuals would need separate `Keyhive` instances, which means separate `BigRepo`s with separate
identity seeds. ADR 006 notes that booting a `BigRepo` with another seed is structurally possible, but each would be
its own repo, store and transport identity, and the node does not run several. Not a feature today.

**6. Discovery today.**
Confirmed from `src/day_cli/cmds/drawers.rs:31-37` and `src/big_cabinet/drawer/known.rs:1-8`: being made a member lets
a node read a drawer, but nothing is fetched until it mirrors it (`day drawers remember <id>`), and remembering is
"bookkeeping, not membership". The id arrives out of band and no inbox exists. One caveat I did not trace: the
keyhive *graph* sync between members is not gated by mirroring in anything I read, so the node's keyhive may already hold
the membership ops. What is certain is that no user-visible record appears. For the ticket, intxt's `dtxt:doc:<drawer>?via=…`
links are not in this tree (the only `via` in `src` is on blob URLs, `src/big_blobs_core/url.rs:84`, and ADR 035
supersedes it), so I take the shape from the operator's description and from the existing peer ticket (`day peers add
<ticket>`). **Proposed minimal invite ticket:**

| Field | Why |
|---|---|
| Drawer id | What to remember and mirror. |
| One or more `via` entries: node key and addresses | Where to connect. Same shape as the peer ticket, so accepting a ticket is `peers add` plus `remember`. |
| Granter's node key | Who invited, and a signature by it over the other fields, so the recipient can tell a real invite from a forged one. |
| Expiry | A ticket exposes addresses, so bound its life. |
| Optional label | A name for the human, never trusted. |

It carries **no** device list, no group members and no access level beyond a display hint: the real access is the keyhive
membership, and the ticket grants nothing (it is not authority, ADR 043 §7). Discovery stays out of the node: the
ticket travels by messaging, and the node only accepts one a person hands it, so there is no inbox to spam.

**7. ATSMS "inbox".**
The specs I read do describe one, in the `at.atsms.inbox` record: "a per-DID singleton ... with an ordered list of
`https:` endpoints for inbound delivery" (`spec/identity-devices.md`). Messages, including a bootstrap invitation, are sealed to a device's published
prekey (`sealed-asym`) and pushed anonymously to a mailbox keyed by `{did}/{deviceFingerprint}`; the owner fetches them
with an authenticated request (`spec/atsms-integration.md`, `spec/sealed-sender.md`). Conversation addresses travel
inside signed group frames, not in public records. I did not find a distinct "you were added" notification in what I
read, and I read these through a summarising tool. So: the mechanism exists and is the same shape as our ticket, with
the difference that ATSMS publishes the inbox endpoint publicly in the PDS and we would hand the person a ticket instead.

### What this means for the design

1. **A granter always learns the devices of the group it is given.** Keyhive offers no way around it, because a CGKA leaf is an
   individual. Privacy comes from the grantee presenting a group that holds only the devices they will expose, which is
   the per-context group of section 3. Say this plainly in the person-facing grant UI.
2. **"Present the grant later" is not available for decryption.** Plan for the granter (or another reader) to seat devices
   as they appear, via `reconcile_doc_readers`, and treat an unseated device as "invited, not yet readable".
3. **Persona unlinkability needs separate keyhive instances.** Do not promise it. Per-context groups hide the other
   devices, not a shared one.
4. **Make the invite ticket the first discovery step**, with the fields above, and keep it outside the node's data
   path. It also fixes today's `UnknownPeer` friction, because it carries the `via` addresses.
5. **Check the two open points before building:** whether our relay policy gates ingest by membership
   (`protocol.rs:1210-1260`), and whether a group can be created whose members the granter never sees.

## Upstream keyhive and keyline (re-checked 2026-11-24)

The operator suspected our fork was stale. It is. Everything above that cites `../keyhive` describes the fork, which
predates keyline. This section re-reads upstream. Where the two disagree, this section supersedes the previous one, and
the supersession is marked.

**What I read, and where keyline is.** Keyline is not a separate repository. It is two crates, `keyline/` and
`keyline_memory/`, plus design docs under `design/keyline/`, all inside `github.com/inkandswitch/keyhive`. Upstream
`keyhive_core` now depends on it (`keyhive_core/Cargo.toml:20`). I cloned `main` read-only into
`/run/media/asdf/p3N/repos/keyhive-upstream` at commit `fef75a5038a0f69533322e3abdb6d2c2fa5d82d1` (2026-11-23, "Bump
versions (#381)"). The fork `../keyhive` (`townframe-changes`) is at `a84137d1f2bdd81cc6c883253a534cc3f0380b5e`
(2026-11-06). Their merge base is `b7968be799705492428c94226ca28042c733a9e0` (2026-10-01), and upstream is 113 commits
ahead of it. The fork has no keyline (`grep keyline` in its two `Cargo.toml` files finds nothing). Paths below are
relative to the upstream clone. A branch named `pairing` exists upstream but dates from 2025 and is unmerged, so I
did not use it.

### a. Can a non-admin member delegate at an equal or lower level?

Yes, and it needs no right at all beyond holding the level. "Anyone may issue a delegation over any subject. The
issuer's effective power over `subject` clamps the result; issuing needs no Admin" (`design/keyline/implementation.md:69`).
The audience receives `min(power, issuer's effective power)` (`implementation.md:62`, and attenuation at
`design/keyline/README.md:158-160`). The code agrees: `Keyhive::sign_delegation` reads the signer's effective power over
the subject, refuses `NoAuthority` if it has none and `Escalation` if the request exceeds it
(`keyhive_core/src/keyhive.rs:1598-1620`). Upstream rejected an admin-only rule on purpose, naming this very case:
"It pushes people toward key-sharing when they want to hand a document to a second device"
(`design/keyline/alternatives.md:82-90`). Admin is needed only to revoke other people's delegations
(`implementation.md:71`). So a `Read` member can grant `Read` or `Relay`, a `Relay` member can grant `Relay`.
The fork allowed the same (ADR 005 cites it), so this is not new.

### b. A grants D to N1, N1 adds N2: does anyone learn N2?

**Yes. The granter and every other reader learn N2, and N2 becomes a CGKA leaf.** Three mechanisms:

- *The delegation travels with the document.* What a peer is sent is `certificates_for_agent`: "the closure of every
  subject it stands over" (`keyhive.rs:2420-2440`). A subject's closure is "`S`, every node with standing over `S`...
  every certificate about those nodes" (`design/keyline/README.md:457`), and "No certificate outside `closure(S)` can
  change any answer about `S`" (`:461`). N1's delegation `N1 -> N2` about `D` is in `closure(D)`, so A receives it the
  next time A syncs D.
- *N2 becomes a leaf.* A `Read` holder may add a CGKA leaf. The authorization rule is: signer has `Read` or higher,
  and an add must carry a key proof, "a prekey operation, signed by the added member" (`keyhive.rs:2921-2946`).
  Seating needs neither `Edit` nor `Admin`. The operations are sent to everyone with standing over D
  (`cgka_ops_reachable_by_agent`, `keyhive.rs:2463-2477`), and the prekey operations of every agent they name go with
  them (`prekey_ops_referenced_by`, `:2313`).
- *Relay is the exception.* A `Relay` delegate is not a leaf (`keyhive.rs:2455-2458`; `design/threat_model.md:33`),
  but its delegation is still in the closure, so the device is still named.

What changes is **timing and control, not secrecy**. A does not need to know N2 at grant time, cannot be asked to
act, and learns N2 only on a later sync. That is the nearest thing to the operator's model ("devices become known
when one comes and asks"). The authority part works; the hiding part does not.

### c. Invites without write access; what must the invitee publish?

Seating and delegating both need only `Read`, as above. A contact card is "an individual's introduction: one of its
prekey operations, verified" (`keyhive_core/src/contact_card.rs:18-20`), and `receive_contact_card` registers the
individual from it (`keyhive.rs:1453`). The invitee must have published, or handed over, at least one prekey operation
that the seater holds. `addable_key` picks a prekey for the document and fetches the proof (`keyhive.rs:845-854`);
without one, the reader is skipped. There is no invite object separate from this: an invite is a delegation plus a
prekey the inviter already has.

**This supersedes the previous section on one point.** In the fork, granting a group failed with `MissingPrekeys` unless
the granter held a prekey for every member (`agent.rs:171-205`). Upstream grants first and seats later:
`issue_implied_cgka_ops` adds a leaf "for each reader who is not currently a CGKA member and whose prekeys this
replica has locally" and skips the rest (`keyhive.rs:756-840`), and `add_member` does not check prekeys
(`keyhive.rs:1570-1596`). So the granter does not need every member's prekeys at grant time. Whoever holds the
tree and a member's prekey seats it afterwards.

### d. Groups, several local agents, hiding membership

- **Groups.** A group has no state of its own: "Its membership is the Keyline certificate set, so a replica stores
  nothing for it" (`keyhive_core/src/principal/group.rs:1-2`). At keyline level "there is no distinction between
  individuals, groups, and documents; they are all merely keys" (`design/keyline/README.md:~52-54`). There is still no
  group contact card, because a card is an individual's prekey op. Granting needs the group to be a known subject
  (`has_agent`, `keyhive.rs:1719-1731`), which means the granter holds at least one certificate about it.
- **Several local agents.** No. `Keyhive` holds one `active: Active` (`keyhive.rs:121`). Two personas on one device are
  still two keyhive instances, as before.
- **Hiding membership from readers.** Not offered. The closure rule above ships every certificate about every node
  with standing to every reader, including through nested roles, and the threat model accepts it: a holder "sees
  ... the membership graph, and the CGKA group's shape and public keys" (`design/threat_model.md:31`, T8 at `:116-118`).
  Upstream's only mitigation is topology: "Roles whose memberships name only individuals keep [the closure] small"
  (`README.md:473`), which is about size, not secrecy.

### e. What changed from the fork that matters here

| | Fork (`a84137d`) | Upstream (`fef75a5`) |
|---|---|---|
| Who may delegate | Member up to own level (ADR 005) | Anyone, clamped to own level (`implementation.md:69`) |
| Granting a group | Needs every member's prekey, else `MissingPrekeys` | Grants; seats those whose prekeys are held, others later |
| Who seats a leaf | Any reader | Any reader, with the added member's key proof |
| Proof carried | `proof` field on delegations | None: "justification is computed at verification-time" (`README.md:129`) |
| Group | Stateful object | A key with certificates, no state (`group.rs:1-2`) |
| What a reader receives | Reachable ops per agent | Closure of every subject it stands over |
| Local agents | One | One |

### The direct answers

- **Can the granter grant one node and have it invite the rest without the granter learning them?** The first half is
  possible: N1 needs only `Read` (a), and N2 needs only a published prekey (c). **The second half is not.** The
  delegation and the CGKA leaf for N2 are in D's closure, so A and every reader receive them on the next sync (b).
  A can still be spared having to know N2 at grant time.
- **Does keyline support invites without write access?** Yes. Delegating needs only holding the level (a), and
  seating a leaf needs only `Read` (b, c).

### What this means for the design

1. **Rewrite the promise.** The earlier recommendation "the grantee chooses what to expose by choosing which group to
   present" stands, but the new fact is that a grantee can also *add devices later* through a device the granter
   already knows, with no further granter action. **Corrected 2026-11-24 (see "Correction: what a reader of D learns",
   below):** the guarantee is not "the granter learns the devices that join the drawer". It is that whoever reads D
   learns every device in the group presented to it, whether or not those devices ever open D, plus any device later
   given standing over D.
2. **Prefer one device per context.** The only way for the granter never to learn N2 is for N2 never to be a member
   of D. If the person wants a second device to read D, the second device is exposed to every reader of D. Offer
   the per-context group as the tool for choosing which devices that is.
3. **Drop two earlier premises.** Granting a group no longer needs the granter to hold every member's prekeys
   (section 4, "someone has to know the group", and section 2's `MissingPrekeys`). The group's certificates must
   still be held, so an introduction bundle for a group stays the group id plus its certificates.
4. **Plan to move off the fork.** Upstream has the delegation model and the late seating the design wants. Moving is a
   separate decision (pins, `big_repo` surfaces, the `Agent`/`Group` types it uses), and I did not assess its cost.
   I did not read upstream's sync protocol or `subduction_keyhive` against it: the claims about what a peer is sent
   come from `keyhive_core`, not from the wire.

## Correction: what a reader of D learns

An earlier line said the granter learns devices "when they join the drawer". That claims too much hiding. If D is
granted to a group G, then G's own membership certificates (G to its devices) travel with D, so **whoever reads D learns
every device in G, including devices that never open D, plus any device later given standing over D.** Nothing else is
hidden. Checked against code, not only the design doc:

- **Upstream.** `closure(D)` is built from `reaches[D]`, the nodes with standing over D, and takes "edges about" each of
  them (`keyline_memory/src/state.rs:304-328`, edges keyed by subject at `:93-95`). G has standing over D, so
  the certificates about G are in it. The design says the same: the closure holds "every node that has ever had standing
  over S ... every certificate about those nodes" (`design/keyline/README.md:457`, `implementation.md:260`: "what sync
  sends"). Keyhive sends it: `certificates_for_agent` is the closure of every subject the peer stands over
  (`keyhive_core/src/keyhive.rs:2420-2440`), and the prekey ops of every agent those certificates name go with it
  (`prekey_ops_referenced_by`, `:2313`). Since a revoked member keeps its place in the positive graph, a device removed
  from G stays visible too.
- **Our fork behaves the same.** `membership_ops_for_agent` walks each delegation head with `bfs_membership_ops`, which
  for a delegation to a group "also enqueues the group delegate's own delegation and revocation heads"
  (`../keyhive/keyhive_core/src/principal/group/membership_operation.rs:586-607`, `:631-650`). It then adds the prekey ops
  of every transitive member (`keyhive.rs:1758-1830`). So the earlier fork-based reading ("a reader receives the group's
  delegations and each member's prekey ops") was right, and "devices appear when they join the drawer" was the mistake.
- **CGKA leaves point the same way.** Every reader of D is a leaf, and all of G's devices are readers of D, so a
  Read-holding replica seats each one whose prekeys it holds (`keyhive.rs:766-840`), whether or not it ever fetches D.

Sections 3 and 5.1 stand as written; the line corrected is in "What this means for the design", item 1 of the upstream
section. What a party to D learns, in one sentence per role:

| Party | Learns |
|---|---|
| Reader of D (granter included) | Every device in each group with standing over D, former members too, and any device given standing over D directly |
| Relay holding D | The same, plus timing and sizes (`design/threat_model.md:31`) |
| A device in G that never opens D | Nothing it did not already hold: it learns D's id and closure when it syncs |

## A public front group (G_pub) for invitations

**The design.** A group G_pub whose one member is a "front" agent, published so anyone can invite it. When the owner
accepts an invitation to D, the front re-delegates D to a private group G_priv or to chosen devices. The aim is that the
inviter never learns the owner's other devices.

**What works.**
- *A group has no prekey.* Only an individual publishes a prekey and a contact card (`keyhive_core/src/contact_card.rs:18-20`).
  So "G_pub's prekey" means the front's card plus G_pub's id.
- *Creating it.* `generate_group` makes the active individual and any coparents the group's admins, by root edges signed
  with a fresh key "that is then discarded" (`keyhive.rs:289-317`). The creating device is therefore always a member and always named
  in the group's certificates. For a relay to be the front, the relay creates G_pub or is a coparent.
- *The front can re-delegate with no write access.* Issuing needs only standing, clamped to it
  (`implementation.md:69`; `keyhive.rs:1598-1620`). A member of G_pub with at least `Read` over G_pub holds `Read` over D
  by the minimum along the route (`README.md:158-160`), so it can sign `front -> G_priv` about D. The group's discarded
  key is never needed, because authority flows through members.

**What the inviter learns, and when.**

| Moment | Learned by the inviter (and any reader of D) |
|---|---|
| Invite: `A -> G_pub` about D | G_pub's id, the front's identity (it is the audience of G_pub's root edge), the front's prekeys (to seat it) |
| Accept: `front -> G_priv` about D | G_priv's id **and every member of G_priv**, because G_priv now has standing over D and its certificates are in `closure(D)`. The same if the front delegates to individual devices: they are named by the certificate and seated as leaves. |

**Verdict: it does not hide the owner's devices from D's readers.** The re-delegation is a certificate about D, so it
is in `closure(D)` for A and every other reader (`README.md:457-461`). No keyline pattern changes that: pinning and
caretakers (`design/keyline/patterns.md:34-70`) change *who can revoke*, not who receives the certificates. What G_pub
does buy is real but smaller: the inviter needs to know only the front to invite, the owner can replace the front without
a new invite, and devices the owner never gives standing over D stay unknown. Two costs: a replaced front stays visible for
good, since the closure keeps every node that ever had standing, and it is an Admin of G_pub.

To keep other devices unknown to D's readers, those devices must not hold D at all. The owner's other devices could reach
D's content through the front, outside keyhive, or the owner exposes a per-context group on purpose. I did not look
at how the front would serve content to them and make no claim about it.

**What our older design says about readers not having to act.** Three places, none of them about devices:

- `docs/wiki/drawer.md:50-58`: a collection group holds the documents; participants are delegated over the group and so
  reach every document added later through one delegation, with no per-participant step. `:62-63`: participants "present
  their standing with the collection group" to ask any other node for a document. `:60-61`: a principal may sign only
  at a level it holds.
- `docs/adrs/019-cabinet-drawers-and-selective-mirroring.md:78-92`: anyone with authority over D may grant D to the
  docs group without the drawer's writers; the grant makes D a candidate, not an entry. Line 90-92 accepts that a reader
  of D may see the group's id, and says that grants "neither the group's roster nor its purpose". There "roster" means the
  list of entries (`:75`). It does not say the group's *members* stay hidden, and under closure they do not (see the
  correction above).
- `docs/fdrs/001-drawers-as-document-collections.md:402`: Read holders can subdelegate Read.

**Does keyline keep it?** Authority, yes: a delegation needs no proof field and is late-bound (`README.md:129`), one
certificate covers every member, and issuing needs no Admin (`implementation.md:69`). Decryption, not entirely: a
reader's key must be seated in D's tree by some Read-holding replica that holds its prekeys (`keyhive.rs:766-840`), which
runs automatically when that replica inserts the certificates and again before it writes (`keyhive.rs:660-690`, `:1099-1148`).
So readers do not have to do anything to be authorised, but some reader has to come online for a new reader to decrypt.
I could not tell which passage the operator remembers; this is what the three documents say.

## Where our causal checkpoint worker is still needed

`src/big_repo/runtime2/causal_checkpoint_worker.rs` and the doc worker serve several cases. For each, what upstream does
in the graph or the CGKA, without a document write. Upstream is `fef75a5`; our fork lacks the two upstream changes below
(`grep PredecessorSecret|InvitationSecret` in `../keyhive` finds nothing).

| Case we serve | Where | Upstream |
|---|---|---|
| **Seat readers** after an admitted delegation or prekey expansion | worker `ReconcileReaders` (`causal_checkpoint_worker.rs:1-12`, `:343-379`) | In the graph, at insert (`keyhive.rs:1099-1148`) and before every write (`:660-690`), by any Read-holding replica with the tree and the prekeys. **Gap:** a prekey arriving alone does not seat anyone (`receive_prekey_op`, `:2844-2858`, does no reconciliation), so a late-prekey reader waits for the next insert or write. Eager seating still needs a worker. |
| **New reader needs old keys** (grant-then-offline, ledger `big-repo-ledger.md:49`) | worker `EnsureCoverage`, `doc_worker.rs:1544-1626` | Handled in the CGKA. Upstream #262 (`1a91c2b`, 2026-10-13) makes a CGKA Add wrap an `Invitation` of the root secrets the inviter can derive, so the invitee "can immediately decrypt" (`beekem/src/operation.rs:134-148`). No checkpoint, no waiting for another writer. **Limit:** only secrets the inviter itself could derive. |
| **Content under ancestor keys after a partition** | `EnsureCoverage`; write-time `ensure_frontier_checkpointed` (`doc_worker.rs:1630-1670`) | #264 (`f132d2e`, same day) has each update carry `predecessor_secrets`: earlier root secrets "encrypted under a key derived from its own" (`operation.rs:180-194`, `:300-307`), so a holder of a later secret works backward. It is merged. Whether any gap remains after both is not established: a test is named for a secret "neither its path nor the chain reaches" that is rebuilt from the tree (`beekem/src/tests/concurrent_updates.rs:90`), which I did not read through. |
| **Root underivable after concurrent updates** (the `RootConflicted` election) | `establish_pcs_root`, `native.rs:345-400` | Upstream has no `RootConflicted` (no match in the tree). A writer who cannot derive the root rotates its own leaf, and a graph with several heads counts as "no current root secret" (`beekem/src/cgka.rs:349-366`, `:367`). The resolving update happens on a write, so nothing background-mints, which is the loop our election damps. The election exists only because our worker mints without a write. |
| **Cold-start sweep** (progress 0) | worker start, `:63-86` | Not a keyhive concern: it covers documents that predate the worker. |
| **Revocation PCS** | not served by the worker (`Revoked` is only acknowledged, `:371-379`) | In the graph: inserting a revocation issues the implied CGKA remove (`keyhive.rs:788-808`), and a write is refused while a non-reader is still in the tree (`:660-690`, `CgkaMemberLacksRead`). |

I searched merged history (`git log` on `fef75a5`) and the five open upstream PRs (#377, #373, #372, #210, #205): none
adds further predecessor-chain or checkpoint work. #210 is our own "townframe changes".

**Which cases would still need something like our worker, on upstream:**
1. Eager seating when only prekeys arrive (a reader seated without waiting for a write).
2. Anything the invitation and the chain do not reach: a partition where the inviter lacked secrets. Unproven either way.
3. Content-level coverage in our own blob format: our checkpoint is a document-level object sealed under the PCS root
   (`native.rs:560-600`), which upstream's CGKA secrets do not replace unless we move to its causal envelopes. That is a
   design choice, not evidence of a gap.

Cases that fall away on upstream: the election, post-grant checkpointing for a new reader, and any revocation handling.

### What this means for the design

1. **Say the true guarantee in the grant UI:** "whoever reads this drawer sees every device in the group you give them".
2. **Do not promise a front group hides devices from the drawer's readers.** Offer it for what it does: a stable invite
   address that the inviter learns, and a place to rotate the front.
3. **If we move to upstream, plan to delete the election and the post-grant checkpoint** and keep a smaller worker for
   eager seating. That needs the two upstream changes, which our fork lacks.
