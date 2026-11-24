# Identity layers: actors, identities and keyhive principals

Status: exploration for the operator's review, nothing decided
Owner: nodules lane
Updated: 2026-11-24
Scope: the lower layer only. It covers what exists, where an actor ends and an identity begins, and what the first
build would be. It does not cover AT Protocol bridging, DID methods beyond what the first step needs, or the end-user
authoring of cartridges (deferred to next week).

## Why now

The operator made identity the focus of the week, together with the relay. intxt's needs (2026-11-24) show the gap.
Everything about a *person* is a claim today:

- The cabinet attests a datum's author as an actor (`DatumAuthor`, `big_cabinet_core/doc.rs:172`). That actor is a
  node's operator, so one person on two devices is two writers, with two notes documents (intxt D70).
- An agent and its operator on one node are the same attested actor (intxt C7). "Agent S for person P" exists only as a
  `creator`/`generator` claim inside content that nobody verifies.
- Usernames that survive a device change (intxt D72), long-lived agent identities (D69) and forge linking (D85) are
  intxt's "later".
- Drawer members are nodes and relays only, with no people, groups or roles.

## What exists

| Thing | What it is | Bound to | Where |
|---|---|---|---|
| **Node key** | Ed25519, generated at `day node init`, held in the keyring. It is also the node's keyhive individual and its cartridge publisher DID (`did:key`). | The device | `big_node/lib.rs` `node_id`; `big_cartridges` publisher |
| **Actor** | A principal in the node's authority tree: the operator (root), subsystems (`documents`, `nodules`, `micros`…), each nodule's actor, and later each paired client and agent (ADR 043). It has its own key pair, sealed by the node, and a certificate the node key issues. It holds capabilities, signs calls, and is what the cabinet attests as an author. URL `dtxt:actor:<node key>/<path>`. | The node | `big_cabinet/authority/actor.rs`; ADR 037 |
| **Keyhive principals** | Individuals (node keys, a recovery principal), groups and documents. They decide who may read or write a drawer's documents and so who syncs what. | Keys and the CGKA | `big_repo`, `../keyhive` |
| **Account and recovery** (proposed) | An account document plus a recovery principal: a keyhive individual created outside the app that holds Admin on the repo's authority groups, so a new device can be re-admitted after all devices are lost. The relay anchors an account on it. | A secret kept outside the app | ADR 005 §1, ADR 006 |
| **Claims** | `creator`, `generator` and `writer` fields inside content. | Nothing, unverified | intxt |

## Proposed delineation

1. **An actor is who acts in this runtime.** It is the operating-system layer: per node, device-bound by construction,
   and never moved between machines (the operator's rule: no node key copied around, because one key would then be a
   multi-writer). When an identity uses a cartridge to change something, the **cartridge's actor** acts. The identity
   is who asked for it, not who did it.
2. **An identity is who someone is across nodes.** It is a DID, and that is all the lower layer assumes. DIDs are the
   bridge to whatever upper layer comes (AT Protocol, a forge, something of our own). It is **data**: records and a
   verifier, with no special case in Dmeta, the cabinet or the authority tree. As far as I know, DID Core has no
   parent/child relation. A `did:web` path such as `…:alice:agent1` is naming only, so "an agent of a person" is
   modelled by delegation, not by the identifier.
3. **A link joins the two.** A record signed by an identity's key says "actor A on node N acts for me", optionally
   narrowed by purpose or expiry. It is stored as a datum in a drawer the identity writes. A verifier turns an
   attested actor into "A, acting for P". That is the use case: intxt's person-level authorship, usernames that survive
   a device change, and one per-writer document per person instead of per device. It goes one way: a link never grants
   authority (ADR 043 §7, "identity is not authority").
4. **Keyhive groups and the recovery principal solve a narrower problem:** how a relay recognises the devices that
   belong to an account, and how a new device is re-admitted after loss (ADR 005/006). An identity may point at its
   account document or recovery principal, but it does not depend on them, and they are not the identity system.

```text
identity (DID, data)  ──link record (signed by the identity)──▶  actor on node N  ──holds──▶ capabilities
                                                                     │
                                                                     └── attested as the author of datums
keyhive account / recovery principal (ADR 005/006): device membership and recovery for relays, referenced, not depended on
```

## Questions for the operator

1. **Where does an identity's signing key live?** The operator does not want one key on several devices. Options:
   - a. **A DID whose key lives on one device or outside the app, with delegations to each device's actors.** The
     identity key is used rarely, to sign links, and the devices act day to day. The recovery principal (ADR 006)
     could double as it, or be separate.
   - b. **A DID method with rotation** (`did:plc`-like): rotation and recovery keys are kept apart from signing keys,
     and the DID document lists the current device keys. This is closer to AT Protocol, but it needs a resolver or a
     PLC-like registry.
   - c. **The identity as a keyhive group** with devices as members. That conflates identity with sync membership,
     which item 4 above argues against.

   Leaning: **a** for now with `did:key`, and **b** when we bridge upward. A link record's shape does not change between
   them.
2. **Is an agent an identity, an actor, or both?** ADR 043's pairing (proposed, unbuilt) makes each agent a sub-actor
   with its own key, approved by the operator or by its parent app (delegated approval). That alone makes "agent S under
   operator P" attested at the actor level. A long-lived agent identity across sessions (intxt D69) would be a DID
   linked to those actors. Leaning: actors now, an agent DID only when an agent must be recognised across nodes.
3. **Should an attested author show the identity, or only the actor?** Leaning: the cabinet keeps attesting actors (no
   change to Dmeta), and readers resolve them to identities through link records on read, so a revoked link stops
   resolving.
4. **Does ADR 043 land as proposed?** It is the first concrete step: per-client and per-agent actors, keys in the
   keyring, pairing approval. It is independent of the questions above.

## First build, once the questions are answered

1. ADR 043 pairing: per-client and per-agent actors (intxt C7 becomes attested). It needs no identity decision.
2. The link record: a Lexicon, a verifier, and a resolver from actor to identity. It uses `did:key` identities first.
3. intxt resolves attested authors to identities for display, usernames and per-writer documents.
4. Relay work under ADR 005/006 (account document, recovery principal), tracked separately because it is about
   membership and recovery, not identity.

Not proposed now: DID resolution over the network, `did:plc` or `did:web` hosting, AT Protocol accounts, forge linking.
