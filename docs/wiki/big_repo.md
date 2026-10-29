# `big_repo`

Status: stub and machine generated. Only points at where things live; expand before relying on it.

big_repo (`src/big_repo/`) is our automerge-repo alternative that supports [Keyhive](./keyhive.md) and [big_sync](./big_sync.md).

## What it does

- Stores automerge documents and syncs each one across peers over [Subduction](https://github.com/dman-os/subduction) (Sedimentree storage, iroh transport).
- Owns the node's Keyhive instance, which decides which peers may see which documents.
- Uses big_sync for the collection level: diffing which documents and heads peers hold, and live change notifications.
- Carries ephemeral messages between peers (`src/big_repo/ephemeral.rs`).

## Where it lives

- `src/big_repo/lib.rs`: the `BigRepo` handle (`create_doc`, `get_doc`, ...).
- `src/big_repo/runtime2/`: the workers behind it (hub, doc workers, Keyhive admission and dispatch).
- `src/big_repo/store/`: the SQLite store.
- Consumers include `big_cabinet`, `big_node` and `daybook_core`.

## Further reading

- [ADR 012: BigSync reconciliation, keys, authorization and delivery](../adrs/012-bigsync-reconciliation-strategy.md)
- [ADR 013: Keyhive access delta stream](../adrs/013-keyhive-access-delta-stream.md)
- [ADR 005: Relay sponsorship, retention and admission](../adrs/005-relay-sponsorship-retention-admission.md)
- [Drawers](./drawer.md) for the membership layer built on top.
