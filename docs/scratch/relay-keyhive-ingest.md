# Relay keyhive ingest: who may send a node keyhive operations

Status: investigation
Owner: nodules lane
Date: 2026-11-25
Scope: whether a node, a relay in particular, accepts keyhive operations from a connected peer that has no standing, and
what bounds what such a peer can make it store. Feeds ADR 005 section 2. Nothing is fixed here.

## Answer

Yes. Any peer the transport authenticates can make a node ingest, persist and project keyhive operations for documents
the node is not a member of. Nothing in the path checks that the sender has standing, and nothing bounds how much it
may send. ADR 005 section 2 is not implemented in any part. Two pins in `src/big_repo/test2/keyhive_ingest.rs` fail
today and assert the safe behaviour.

## The path

1. Transport accept. `Accept::accept` in `src/big_node/peers.rs:306` asks `PeerPolicy::accept`. `RelayPolicy::accept`
   (`src/big_node/peers/policy.rs`, `RelayPolicy` impl) and `MirrorPolicy::accept` both return `true`: the doc comment
   says access, not connection, is the gate. The repo then takes the connection (`accept_connection_iroh`).
2. Subduction connection policy. `BigRepo` boots `SubductionKeyhive` as the policy (`src/big_repo/lib.rs:530`). Its
   `authorize_connect_with` (subduction `subduction_keyhive/src/policy.rs`, at the pinned rev) only checks that the
   peer id is a valid Ed25519 key. The doc comment says so on purpose: a fresh peer must be able to onboard. Per document
   fetch and put are gated (`authorize_fetch_with`, `authorize_put_with`), but that governs sedimentree data, not
   keyhive events.
3. Keyhive message handler. `SendableKeyhiveHandler::handle` (`subduction_keyhive/src/handler.rs`) calls
   `KeyhiveProtocol::handle_message` for every `KeyhiveMessage` on an authenticated connection.
4. `handle_message` (`protocol.rs:798`) verifies the signature and that the inner `sender_id` matches the signer, then
   ingests an attached contact card (`ingest_contact_card`), then auto-registers the sender as a peer if unknown
   (`add_peer`). No standing check.
5. `handle_sync_ops` (`protocol.rs:1210`) hands `ops` straight to `ingest_events` (`protocol.rs:1708`). It does not
   check that the ops were requested in an earlier `SyncRequest`/`SyncResponse`; an unsolicited `SyncOps` is
   ingested the same way. (The honest sender filters what it offers by what the receiver may see, but that filter is
   the sender's, so it does not constrain a hostile one.)
6. `ingest_events` deserialises, drops events keyhive already holds, then calls
   `Keyhive::ingest_unsorted_static_events_with_pending_progress` (`keyhive_core/src/keyhive.rs:3432`). Whatever it
   cannot place stays in keyhive's in-memory `pending_events`, which is replayed in full on every later ingest.
7. `persist_incorporated_event_bytes` (`protocol.rs:~1960`) then writes **every** event of the batch to the WAL via
   `save_event_bytes`, pending or not, and admits the resolved ones to the admission log through
   `KeyhiveIncorporationSink::admit` (`src/big_repo/store/sqlite.rs:323`). `report_newly_admitted` keeps unresolved ones
   in `unadmitted_inserted`. Boot replays the WAL (`ingest_from_storage`), so pending junk survives restarts.

Everything above runs identically on a relay: `RelayPolicy` only changes what the node wants (`wanted`), never what it
ingests.

## Per operation kind

What keyhive verifies: signatures (`Signed::try_verify`) on every kind, and for some kinds the graph. It does not verify
that the signer is a member of anything the node holds.

| Op | Who may send | Verified | Stored or pending | Bound |
|---|---|---|---|---|
| Delegation, founding (no proof, issuer is the subject key) | Anyone | Signature; issuer equals subject key (`group/state.rs add_delegation`) | For an unknown subject it creates a new Group, or a Document when `after_content` names heads (`keyhive.rs:2379` onward). Persisted, admitted, projected, notified. | None. Each fresh keypair is a new subject. |
| Delegation, chained, on a known subject | Anyone | Signature, proof lineage, no escalation, signer reaches the proof's delegate | Valid ones are admitted. A failing one is not rejected: every error but prekey errors goes back to `pending_events`. | None on pending size. |
| Delegation, chained, proof unknown | Anyone | Signature only (`MissingDependency`) | Pending forever, persisted to the WAL. | None. |
| Revocation | Anyone | Signature, dependencies | Same pending behaviour as delegations. | None. |
| Prekey ops (add, rotate) | Anyone, for their own key | Signature, `op.verifying_key() == individual id` | Creates an Individual per signing key (`receive_prekey_op`, `keyhive.rs:2326`); ops accumulate in the individual's prekey state. Persisted and admitted. Errors are dropped, not pended. | None per key or in key count. |
| CGKA ops | Anyone | Signature; document must exist; `cgka_op_is_authorized` checks the op against the membership graph (`keyhive.rs:2723`) | Unknown document or unresolved authorisation: pending forever, persisted. `Denied` is also treated as pending by the ingest loop (it only special-cases prekey errors). | None. |
| Contact card (attached to any signed message) | Anyone | Signature on the prekey op; the card's key is not compared to the sender's | Creates the Individual and persists the prekey op (`ingest_contact_card`). A peer may attach a card for another key if it holds a valid op for it. | None. |
| `SyncRequest` / `SyncCheck` | Anyone registered | Signature | Answered from the pair-visible set; cost is CPU. Not examined further here. | None. |

## Is there a bound

No. Specifically:

- No per-peer or per-session event counter, no byte budget, no cap on keyhive graph size. The only limits are the wire
  frame size and the disk.
- `pending_events` is an unbounded `Vec` that every ingest replays in full, so each junk event also taxes every later
  honest ingest. The WAL stores the same junk, and the replay on boot restores it.
- `ingest_events` fails the whole batch if one event does not deserialise, but a batch of valid-looking events from a
  stranger is accepted.
- The mitigation that does exist is indirect: the relay serves sedimentree data only to peers with access, and a
  document founded by a stranger has no relay-readable data. That does nothing about graph, WAL and pending growth.

Red herring to avoid: the `big_sync object task failed; rescheduling` waves in AGENTS.md are about a local keyhive that
has not yet pulled membership. This is the opposite problem, a keyhive that takes anything.

## Pins

`src/big_repo/test2/keyhive_ingest.rs`, run under the `Pair` harness. A left node (the stranger) creates a private
document nobody is granted, takes the keyhive events that appended to its log, signs them into an unsolicited `SyncOps`
and gives it to the right node's `KeyhiveProtocol::handle_message`, which is what the transport handler does.

| Test | Result today |
|---|---|
| `stranger_cannot_found_a_document_on_a_peer` | Fails: the receiver now holds the stranger's document in its keyhive graph. |
| `stranger_cannot_park_pending_events_on_a_peer` | Fails: the receiver persisted 2 unresolvable CGKA events. |

Limit of the pin: `Pair::boot` runs a keyhive sync first, so the receiver already knows the stranger's agent through a
contact-card exchange. The pins therefore show that even a *known* agent with no grant is not bounded. A truly unknown
peer behaves the same, because no step above looks at whether the sender is known. A `big_node` relay e2e would show
the same through `RelayPolicy`; the ingest code is shared, so it was not duplicated.

## What ADR 005 section 2 needs

The ADR names two measures. Neither exists, and the ADR leaves the mechanism open ("a subduction_keyhive change tracked
separately"). From what the path shows:

1. Refuse unknown peers. The seam is `KeyhiveProtocol::handle_message`/`handle_sync_ops`, before `ingest_events`, not
   `PeerPolicy::accept` (the transport cannot tell a keyhive message from a sync message, and the ADR wants keyhive
   refused while onboarding still works). It needs an admission predicate supplied by the embedder (relay: known agent or
   valid account anchor; default policy: always yes) so the library stays policy-free. Design questions: what is an
   account anchor on the wire, and how does the first contact card arrive out of band (ADR 005 section 1).
2. Refuse unsolicited `SyncOps`: accept ops only for a `request_id` this node issued, and only for hashes it asked for.
   That needs per-request state in the protocol and closes the stranger-pushes-ops hole independent of the allow-list.
3. Bound growth. A per-peer, per-session event and byte budget at the same seam, a cap on `pending_events` (evict
   or reject, and stop replaying all of it every ingest), and a cap on WAL rows per source peer (`source_id` is already
   recorded on admission rows, `save_event_with_source`). The ADR says the relay policy declares the limits, so the
   policy hook needs a `keyhive_budget(peer)` answer.
4. Stop treating `Denied` CGKA results and invalid chained delegations as pending. They can never resolve and should be
   dropped with a counter, as prekey errors already are.
5. Decide the cost to honest onboarding: a founding delegation from a new device must still reach a relay that serves
   it, so "unknown" has to be defined by the identity layer (see `identity-layers.md`), not by keyhive membership alone.

The first three are `subduction_keyhive`/`keyhive_core` changes (our forks), with the budget and allow-list wired from
`big_repo`/`big_node`. Both pins should go green with 1 and 2 alone; a bound without them would still leave the
document-founding hole for a sender within budget.
