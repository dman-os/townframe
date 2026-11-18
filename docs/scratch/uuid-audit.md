# UUID audit

Scope: every UUID use in the Rust code under `src/` (non-test code classified in depth, test fixtures grouped); excludes `target/` and generated code. No `Uuid::from_u128`, `new_v5` or `new_v8` exists outside tests.
Updated: 2026-11-19

Ledger item: "UUID audit" in `docs/scratch/prod-readiness-ledger.md`. Static read only; nothing built or run.

Generators found in non-test code: `Uuid::new_v4` (about 45 sites), `Uuid::now_v7` (9 sites, plus `Uuid7::new()` wrapping it), `Uuid::from_bytes`/`from_slice`/`parse_str` (decoding only). SQLite stores UUIDs only as `TEXT` columns; no UUID is an `INTEGER`/rowid, so index locality is not currently exploited by any id.

## Summary table

| Location | Identifies | Travels | Version | Concern |
|---|---|---|---|---|
| `big_cabinet_core/dmeta.rs:87` `Uuid7`, `Uuid7::new` | actor, doc (`docUuid`), operator, namespace, grant subject | SQLite TEXT PK (`authority_actors.uuid`), signed attribution cert, dmeta in synced docs (peers), logs | v7 | HIGH: leaks creation ms to peers; actor uuid bound to a key |
| `big_cabinet/authority/open_actor.rs:88` | actor uuid | `authority_actors` PK TEXT; cert; wire | v7 | same; uuid stands beside `public_key` |
| `big_cabinet/doc/{create,write,branch}.rs` | `docUuid` of a document | dmeta in doc, synced | v7 | leaks doc creation time (already in `created_at`) |
| `big_cabinet_core/authority.rs:25` `CapId` | capability id (bearer-like) | wire to clients, `nodule_applied_grants.capability_id` BLOB, redacted Debug | v4 | LOW: `Default` yields nil uuid; entropy 122 bits OK |
| `service_client_native_rs/xrpc/ws.rs:62` | WebSocket frame mask key | wire (peer-visible) | v7, first 4 bytes | HIGH: bytes are the timestamp, not random; violates RFC 6455 |
| `service_client_native_rs/xrpc/client.rs:239` | `Sec-WebSocket-Key` nonce | wire | v7 | LOW: should be 16 random bytes; v7 is timestamp-prefixed |
| `service_core_rs/context.rs:67` | span id (low 8 bytes of v7) | wire (traceparent) | v7 | LOW: random half used; 2 variant bits fixed |
| `service_core_rs/context.rs:107,134` `request_id` | request id | serialized in `RequestContext`, logged | v7 | MED: client-visible creation time; fine on a trusted boundary |
| `service_core_rs/cancellation.rs:114,250` | admission epoch, cancellation token | in-process, wire in context (`bound_to`) | v7 | LOW |
| `big_automata/replay/client.rs:375` `SessionId` (`wire.rs:14`) | replay session, client-chosen | wire to server, server `HashMap` key per principal | v4 | LOW: right version; see Q1 on collision handling |
| `big_automata/outbox.rs:25`, `big_sync_core/outbox.rs:52` | command correlation id | local queue, handed to driver | v4 | LOW: local only; could be a counter |
| `wflow_core/kvstore/snapstore.rs:100` | snapshot blob id | KV key (blob_key), snapshot metadata JSON | v4 | LOW: v4 key in an append-heavy KV; see F6 |
| `daybook_core/lib.rs:105`, `stores.rs:223,291` `VersionTag.version` | per-update version of a store item | Automerge doc (synced to peers), 32-byte `version||actor_id` | v4 | LOW: fine; see F7 on collision |
| `daybook_core/tables.rs` (~20 sites) | table, tab, panel, window ids | Automerge/store docs, synced; some as string keys | v4 | none |
| `daybook_core/repos.rs:264,559`, `big_repo/changes.rs:698-830` | listener registration id | local only | v4 | none |
| `daybook_core/drawer/dmeta.rs:297,433` | facet uuid, meta uuid | Automerge dmeta (synced); stored as string | v4 | LOW: v4 string in doc, fine |
| `daybook_core/drawer/mutations.rs:85` | add idempotency key (defaulted) | local + command log | v4 string | LOW |
| `daybook_core/repo.rs:409,429,438`, `sync/bootstrap.rs:578,619,797` | repo id, checkout id (`dcheckout_`+base58), temp dir | local FS, bootstrap handshake, logs | v4 | LOW |
| `daybook_core/rt.rs:757,2251,2262` | workload id, dispatch/job id suffix | local, logs | v4 | none |
| `big_repo/runtime2/keyhive_dispatcher.rs:65` | subscription id | local | v4 | none |
| `big_repo/runtime2/native.rs:1279` | create-doc log correlation | logs only | v4 | none (value unused beyond log) |
| `btress_sysadmin/server/session.rs:125,135` | session `sid` | signed cookie (`SignedCookieJar`) to browser | v4 | LOW: v4 correct; sid alone is not the secret |
| `big_cabinet/doc/sqlite/migrations/001_commands.sql`, `drawer/.../001_drawers.sql:32` | `command_id TEXT PRIMARY KEY`, `actor_uuid TEXT` | SQLite | caller-chosen | MED: see F4 |
| `big_nodule/sqlite/.../001_nodules.sql` | `actor_uuid TEXT UNIQUE` | SQLite | v7 (actor) | follows actor uuid |
| `daybook_types/macros.rs:41` | uniffi bridge for Uuid as 16 bytes | Kotlin FFI | any | none |
| Tests (about 15 `from_u128`/`from_bytes([..])` fixtures, `new_v4` in tests) | fixtures | local | n/a | none |

## Findings, ranked

### F1 (high) WebSocket mask key taken from a v7 timestamp
`service_client_native_rs/xrpc/ws.rs:62` uses `Uuid::now_v7().as_bytes()[..4]` as the client frame mask. The first 4 bytes of a v7 are the top 32 bits of a 48-bit millisecond timestamp: they change once per about 65 s. RFC 6455 section 5.3 requires an unpredictable per-frame mask; masking exists to defeat cache-poisoning proxies, and a predictable mask defeats it. The mask is also reused across frames within a minute.
Fix: `rand::random::<[u8; 4]>()` (or `getrandom`) per frame. Same file family: `client.rs:239` should send 16 random bytes for `Sec-WebSocket-Key`; v7 works by accident but is the wrong tool.

### F2 (high) v7 on the peer-synced cabinet path (`Uuid7`)
`Uuid7` is chosen deliberately (validated on parse, `unix_millis()` exposed) and is used for actor uuids, `docUuid`, operator and namespace ids. These are written into dmeta and attribution certificates that sync to peers (`big_cabinet_core/dmeta.rs`, `dmeta/attribution.rs:186`) and logged. Each leaks millisecond creation time and ordering of actors and documents to every peer that sees the doc, and correlates actors with documents by time. `created_at` is already in `DocIdentity`, so doc creation time is no extra leak, but actor creation time and actor-to-actor ordering are. The only consumer of the timestamp is a test (`dmeta/tests.rs:196`), and storage uses TEXT PKs in tables of a handful of rows (`authority_actors`), so locality gives nothing.
Recommendation: decide per role. Keep v7 only where the time is already public. Move actor uuids to v4, which needs `Uuid7` to be relaxed or renamed (breaking change is acceptable per AGENTS.md); it is required by the "v7 only" check in `TryFrom<Uuid>` and `attribution.rs:149-152`. See Q2 first, since the format may be a spec requirement.

### F3 (medium) actor uuid stands beside, and duplicates, the actor public key
`authority_actors` holds `uuid` (PK) and `public_key` (UNIQUE), and the certificate binds both. A FIXME at `attribution.rs:30` already notes "actor uuid in attribution" is dubious because actor uuids are doc-specific. A UUID adds an identifier peers must trust the node to keep unique, with nothing but 74 random bits of v7 (rand_b + rand_a) to prevent collision across actors; collision of two actors' uuids would be an attribution confusion, and the insert does not check beyond the PK on the local node. If the uuid does not need to be stable across key rotation, the public key (or a hash of it) is a better actor id and removes the version question. Not addressed here per the FIXME rule; flagged as a candidate.

### F4 (medium) caller-chosen `command_id` as PRIMARY KEY with no format check
`command_id TEXT PRIMARY KEY` in the doc commands and drawer ops tables takes whatever string the caller supplies (idempotency key; `mutations.rs:85` defaults to v4). The request digest column presumably catches a reuse with different content; confirm. A malicious or buggy client choosing a colliding id gets "same command" semantics. Recommendation: parse to a typed uuid at the edge, and confirm collision with a different `request_digest` fails loudly rather than replaying the old receipt.

### F5 (low) v7 request ids and cancellation tokens cross the wire
`RequestContext.request_id` is serialized and logged (`context.rs:107`); clients and servers see each other's creation time. On a trusted service boundary this is harmless and helps log ordering. Flag only if untrusted clients reach it (btress_sysadmin browser path does not use it as far as I read). Span id from `now_v7()[8..]` is fine but has the 2 variant bits fixed; use `rand::random::<u64>()`.

### F6 (low) v4 blob keys in snapstore
`snapstore.rs:100` writes each snapshot under a fresh v4 key into a KV store. If the KV backend is a B-tree with a prefix scan or append-heavy writes, v4 scatters writes; v7 would cluster them. Blob ids are local and never leave the node, so v7 would not leak. Low because snapshots are written rarely.

### F7 (low) uniqueness by randomness without check
v4 is relied on, unchecked, for `VersionTag.version` (collision would make two edits compare equal; 122 bits makes it practically impossible), replay `SessionId` (server `insert` at `server.rs:193` overwrites silently if a client replays an id it did not mean to), outbox command ids, and listener ids. Only the session id insert deserves a look: it should reject an existing id for a different principal-scoped state. Principal scoping already isolates clients, so cross-client collision cannot occur.

### F8 (low) `CapId` derives `Default` giving the nil uuid
`authority.rs` derives `Default` on a capability id. A defaulted capability id is a predictable value. Remove `Default` unless a test needs it (several callers use `from_bytes([1;16])` as fixtures in non-test files `doc/query.rs:171`, `drawer/get.rs:56`, `drawer/grant.rs:45`; check those are inside `#[cfg(test)]`).

### Checked and fine
Replay `SessionId` as v4 is the right choice: it is client-chosen, reveals nothing, and `reset` takes a fresh id so the old one is never reused (`client.rs:375`). Browser `sid` is v4 inside a signed cookie. Table/tab/window ids are v4, synced, no locality need. Local-only ids (listeners, outbox, subscriptions, temp dirs) are v4 and harmless.

## Open questions for the operator

1. Replay `SessionId`: should the server cap sessions per principal and reject a session id already held by a different state, or is overwrite on `insert` intended (resume after reset)?
2. Is v7 for `Uuid7` (actors, docs) required by the cabinet spec/ADR (e.g. for ordering of `docUuid` "smallest wins" in `dmeta.rs:218`), or only a convenience? The "smallest minted uuid" rule uses ordering, which needs a v7 time prefix to be meaningful; v4 would make it arbitrary but still deterministic.
3. Should actor identity be the public key (or its hash) instead of a uuid (FIXME at `attribution.rs:30`)?
4. Is the ws client path used against untrusted proxies? If so F1 is a vulnerability, not hygiene.
5. Is `request_id` ever exposed to untrusted clients or logs shared outside the operator?
6. Does `command_id` come only from authenticated, typed callers, or can it be free text from a plug?
