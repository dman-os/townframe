# NSID audit: what in the tree uses the Lexicon namespace for non-Lexicons

Status: Draft
Owner: nodules lane
Updated: 2026-11-18
Scope: Every NSID-typed or NSID-shaped identifier on `stack/node-59-adr-d56` and claytxt's `origin/feat/desktop-base`, classified. Asked by the operator: NSIDs are atproto's single namespace for Lexicon schemas, so a non-Lexicon thing sharing it is a collision risk. Read-only survey; two findings spot-checked by the manager (rows marked ✓).

Root cause: one validated `Nsid` type (`datum_core/nsid.rs`) is documented as "the one name format for contracts and schemas (ADR 028 §4)", so it is reused for every reverse-DNS name.

**Fine:**
- Lexicon document ids (cartridge `lexicons`, the lexicon registry slot).
- Method/operation ids (`#method`, `implements`, `grant.method/<nsid>`): each names a procedure/query/subscription Lexicon.
- Cartridge and package `authority`: a DNS-owned prefix, atproto's own convention.
- WIT packages (`townframe:dayframe@0.4.0`) and `dispatch.local`: separate id spaces, used correctly.

**Misuse, most consequential first:**
1. **Collection contract ids** (`clay_mdk` `CollectionContract.id`, `Request.contract`, `collection_requests`): e.g. `org.example.clay.shell.roots`. A collection contract is a runtime shape (selector/key/value schemas plus offered capabilities), not a Lexicon, and nothing requires one to exist. Their `selector/key/value_schema` ids name CBOR shapes, also with no Lexicon required.
2. **Runtime-defined `org.example.clay.*` contracts:** explicitly "only ever requested, never declared", so they occupy an authority's namespace with no Lexicon and no owner.
3. **Call and presentation contracts:** micro call contracts are documented as Lexicons, but none is required. `org.example.clay.presentationComponent` ✓ (`clay_guest_session/capabilities.rs:43`) names a runtime kind, not a schema.
4. **Instance ids typed as `Nsid`:** `BindingGrant.collection` (a declared collection *instance*) and `.contract` share one type (`dayframe_composition/bindings.rs:45-47`).
5. **Tags that aren't valid NSIDs at all:** `clay.micro.bindings` ✓ and `clay.micro.install` ✓ (`dayframe_composition/bindings.rs:29`, `lib.rs:184`), sitting in the same tag space as NSID tags.
6. **Host-internal datum tags** (`org.example.daybook.cartridges.grant.*`, `...cartridges.install`, the sidecar tag): record-kind names with no Lexicon behind them.

**Collision points:** an operation and an authority prefix can be the same string (`org.example.tags`; today kept apart by two grant datum kinds); a sidecar is keyed by the same NSID as the Lexicon it annotates; datum tags share the namespace with record Lexicons; collection instances and contracts share a type.

**Direction (manager, to settle with claytxt and the operator):** an NSID names a Lexicon, always. A thing that is a schema gets a real Lexicon (records, procedures, or a def inside one, referenced as `nsid#def`). A thing that isn't a schema (a collection instance, a runtime kind, a host-internal tag) gets its own id space and type. Host datums either publish their Lexicons or stop looking like NSIDs.
