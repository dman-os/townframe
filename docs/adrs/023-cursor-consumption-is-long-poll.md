# ADR 023: Cursor consumption is long polling

**Status:** Accepted. Not yet applied to Cabinet. `ObserveDatums`,
`ObserveDocAvailability` and `ObserveCapability` are still `stream` operations, and
their conversion is tracked below. big_sync already follows this rule
([ADR 012](012-bigsync-reconciliation-strategy.md) §9).

## Decision

Any service surface that exposes a cursor is consumed by **long polling**, not by
server push:

- the caller names a cursor, a page limit, and a hold;
- the server answers at once when it has data past the cursor. When it has none, it
  holds the request until data arrives or the hold expires;
- every answer, including an expired hold, carries the cursor to send next and
  whether the reader is caught up. An expired hold is an ordinary empty page, not an
  error;
- the caller re-issues. A caller asks for a hold only when it is caught up; while it
  is behind it pages with `hold_ms = 0`.

A server-push subscription (a `stream` operation in `service_core_rs`) needs a stated
reason in its own ADR: why the data has no cursor, or why polling cannot meet a
requirement that is written down. Transport latency alone is not a reason. A transport
may carry polls over a WebSocket where that is cheaper, but that is a transport
optimisation, not a second protocol shape.

This generalises ADR 012 §9, which made the same decision for big_sync event
delivery, to every service surface in the repo.

## Why

- **The consumer's request rate is the flow control.** There is no shared buffer
  between producer and consumer whose fullness acts as back-pressure, and no bounded
  `mpsc` per subscriber that a slow consumer fills.
- **Any interruption resumes from the cursor.** A dropped connection, a restart on
  either side, or a crashed consumer all recover the same way: the next poll names the
  last cursor the consumer durably processed.
- **The server keeps no per-consumer session.** Nothing has to be opened, kept alive,
  or torn down, and a poll can land on any process that can read the store.
- **Revocation is the next request failing authorization.** Each poll is authorized on
  its own, so there is no in-stream capability watch and no "access lost" message to
  order against data.
- **One shape covers both paging and waiting.** A current-state query is a poll from
  the beginning with no hold, paged until caught up. A watcher is the same poll with a
  hold once caught up.

## Rules for a long-poll surface

1. **The cursor is the only consumer state, and the consumer owns it.** The server may
   cache target metadata to avoid resending it on every page, as ADR 012 §9's
   "persist target metadata, not delivery state" does. It never holds delivery state:
   no delivered sets, no queued events, and no server-side cursor that a consumer
   cannot reproduce.
2. **An event stamped with cursor `r` promises that everything at or before `r` has
   been delivered.** A server never stamps an event with a cursor ahead of the page it
   read. For example, stamping with "latest revision of the store" breaks resume,
   because the consumer then skips everything between the page and the stamp.
3. **Pages are bounded, and holds are capped by the server.** The caller's limit and
   hold are requests, which the server clamps. big_sync's caps are 1024 events and
   15 s (`src/big_sync/rpc.rs:34–51`); a new surface starts from those values and
   states its own if they differ.
4. **A hold wakes on anything that can change the answer.** That means new data past
   the cursor, and also an authority change that can turn the answer into `Denied`,
   so that a revocation does not sit behind a full hold.
5. **Held requests have their own admission class.** A hold occupies a request slot
   for its duration. A surface whose polls hold must not draw from the same permit
   pool as ordinary unary calls without a cap. Today the XRPC router admits every
   handler, unary or subscription, through one 64-slot semaphore and refuses when it is
   full (`src/service_utils_rs/xrpc/router.rs:238–251`, `:493`, `:612`). A subscription
   holds its permit for its whole life; a held poll holds one for up to the hold. Held
   polls therefore need a separate class or cap there, as big_sync caps its in-flight
   handlers (`src/big_sync/rpc.rs:81–88`).
6. **Delivery is at least once.** A consumer applies events idempotently by key and
   continues from `through`. A multi-page read is not a point-in-time snapshot: later
   pages may carry newer cursors. That is the contract, not a defect.
7. **In-process consumers use the same operation.** A worker in the same process polls
   through the Direct client. There is no Rust-only subscription path beside the
   protocol.

## What already fits

- big_sync's `replay_page` (`src/big_sync_core/rpc.rs:37–45`, request at `:801–839`;
  responder in `src/big_sync/part_store.rs:497–870`) is the reference implementation:
  answer with data at once, otherwise open a reader and `select!` it against the hold.
- The local reader contract already has this shape. `RevisionedStoreReader::next` and
  `KeyedFrontierReader::next` return a bounded page or wait inside the call
  (`src/big_sync_core/revisioned_store.rs:28–79`, `keyed_frontier.rs:113–122`; the
  SQLite reader waits on a `Notify` at `src/big_sync/keyed_frontier/sqlite_read.rs:191–224`).
  A long-poll handler is that reader plus authorization plus a clamped hold.

## What changes

- **`ObserveDatums` and `QueryDatums`** become one unary `PollDatums` operation
  ([ADR 025](025-cabinet-datum-predicates-and-polling.md)). The `DatumPump` (one task
  and one `mpsc(64)` per subscription, `src/big_cabinet/doc/observe.rs:72–96`) and its
  internal `ObserveCapability` subscription go away.
- **`ObserveDocAvailability`** (`src/big_cabinet_core/doc/availability.rs:37`) and
  **`ObserveCapability`** (`src/big_cabinet_core/authority.rs:313`) have no cursor
  today. Converting them needs a revision on availability state and on capability
  status. They are follow-ups. Neither is needed on the `PollDatums` path once each
  poll re-authorizes.
- New surfaces, starting with big_blobs, are designed as long polls from the start.

## Consequences

- Positive: one consumption model across big_sync, Cabinet and big_blobs. Resume and
  revocation need no special cases, and servers hold no per-consumer state.
- Cost: an idle watcher sends one request per hold period. With a 15 s cap that is
  negligible next to big_sync's existing traffic.
- Cost: latency after a hold expires is one round trip. A consumer that is behind
  never waits on a hold, so this applies only at the tip.
- Cost: held polls need the admission class described in rule 5 before any surface
  holds over XRPC.
