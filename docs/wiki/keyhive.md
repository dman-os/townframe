# Keyhive

Status: stub; machine generated. Only points at where things live; expand before relying on it.

Keyhive is the membership and access control layer for documents.
We consume the [dman-os/keyhive](https://github.com/dman-os/keyhive) fork (`keyhive_core`, `keyhive_crypto`, `beekem`), pinned by rev in the root `Cargo.toml`.

## What it gives us

- Principals: individuals, groups and documents, all identified by 32 byte ED25519 public keys. A document id is a Keyhive document principal (see [Docs](./docs.md)).
- Membership and delegation between principals with the `Relay`, `Read`, `Write` and `Admin` access levels. [Drawers](./drawer.md) are built on Keyhive groups and use this vocabulary for their roster entries.

## Where it lives

- [big_repo](./big_repo.md) owns the Keyhive instance:
  - `src/big_repo/keyhive.rs`: `BigKeyhiveHandle`, `BigKeyhiveGroup` and the agent/authority types.
  - `src/big_repo/keyhive_storage.rs`: archives on disk; events and AEAD-encrypted secret material in SQLite.
  - `src/big_repo/handler.rs`, `src/big_repo/keyhive_conn.rs`: Keyhive sync between peers over the `subduction_keyhive` protocol.
  - `src/big_repo/access_policy.rs`: Keyhive-governed access to big_repo's part store.
  - `src/big_repo/runtime2/keyhive_*.rs`: admission, dispatch and the access stream.

## Further reading

- [ADR 013: Keyhive access delta stream](../adrs/013-keyhive-access-delta-stream.md)
- [ADR 006: Recovery principals](../adrs/006-recovery-principals.md)
- [ADR 012: BigSync reconciliation, keys, authorization and delivery](../adrs/012-bigsync-reconciliation-strategy.md)
- `.agents/skills/fork-pinning-and-upstream-sync/SKILL.md` for how the fork is pinned.
