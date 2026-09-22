---
name: message-ordering-audit
description: Audit a hub command/event/mailbox message surface for causal-ordering defects - one fact split across two channels, a completion that outlives its own effect, a swallowed failure that never reaches its waiter, or a fence that cannot see work routed behind it. Use when a caller receives a success receipt for work that did not happen, when a "reconciled"/"fully synced"/"applied" verdict precedes the state it claims, or before trusting any completion message.
---

# Message ordering audit

Use this when a symptom says "something was reported done that was not done": a success receipt for
content that never applied, a reconciliation verdict that precedes projection, a sync failure against a
connection that was just closed, a quiescence snapshot that misses work which was already submitted.

## The rule

In an actor hub with more than one inbound channel, **a fact is ordered only if both of its halves are
enqueued by the same task** — or ride one channel from one sender. Anything else is a race, even when it
usually works.

`big_repo`'s hub makes this concrete: handlers are synchronous and enqueue with `try_send`, and the
machine loop polls **commands before events** (`select_biased!`). So:

- a completion emitted as a command can overtake the event that carries its own content;
- two events from different tasks are unordered, however "FIFO" the channel is;
- a single FIFO mailbox only orders what one sender sent — count the senders before trusting it.

## Sweep procedure

1. Enumerate every variant of each inbound enum (`Runtime2Cmd`, `Runtime2Evt`, plus worker mailboxes
   such as `DocWorkerMsg`).
2. For each: find its sender(s) and its handler, with `file:line`. A variant with exactly one sender
   cannot be inverted against another variant from that sender; two senders is where to look.
3. For each, ask: **which other message is the other half of this fact, and which channel does it
   travel on?** Completions are the high-yield cases: what does this completion resolve (waiter,
   receipt, claim, in-flight counter), and is the state that verdict describes already visible?
4. Report a table: `variant | channel | sender(s) | handler | coupled message(s) | verdict + why`, plus
   an explicit one-line "checked, safe" list. An audit that silently skips variants is not an audit.
5. Verify the audit's claims in the code before fixing: a claim like "X is removed only by Y" is
   exactly the kind of statement that is wrong. On this codebase an audit misattributed one site
   (`CloseConn` already deregistered inline) and the site that *was* broken was a different inversion
   (a late `ConnEstablished` re-registering a closed connection).

## The patterns, with the fixes that were landed here

1. **Completion overtakes its own content.** A round's completion was a command while its received
   commit list travelled as an event; the command was handled first, so the receipt resolved from an
   empty reconsider. Fix: move the completion onto the same channel as its content
   (`DocSyncRoundDone`/`DocSyncFailed` became events), so the content apply is routed before the
   receipt resolves.
2. **A swallowed failure resolves as success.** Routing the content failed (mailbox closed), the error
   was logged and dropped, and the later completion still resolved the caller with a success receipt.
   Fix: never swallow; fail the waiter, make the failure sticky for that round so the completion cannot
   overwrite it, and type the error (`SyncDocError::WorkerUnavailable`, retryable). Note which failure
   modes the channel actually has: an `async_channel::unbounded` mailbox can only fail when *closed*,
   so a comment claiming "closed or full" is already a lie about the system.
3. **A completion that does not check the state it depends on.** A keyhive round resolved its waiters
   from the completion alone, relying on the admission event having been enqueued first. Fix: carry the
   evidence (a store-side admission watermark stamped into the completion) and *defer* resolution until
   the watermark reaches it — turning "the producer is ordered" into "the consumer cannot resolve
   early". The deferral must be released by the watermark event and must not wedge shutdown (check that
   it holds no tracked work, and that close/cancel paths still fail its waiters).
4. **A completion outruns a transition owned by another task.** `close_connection` returned before the
   hub deregistered the peer, so an immediate sync started a doomed round; and because commands are
   polled before events, a close could be processed *before* its own establishment, letting the late
   establishment re-register a dead connection. Fix: resolve after the transition, share one
   deregistration helper between the paths, and guard the late event with the connection's end flag.
5. **Work routed behind a fence is invisible.** A quiescence probe fences workers, but a fence ack only
   covers work enqueued before the fence; a command handled behind the probe can route work behind the
   fence and the probe still resolves. Fix: treat *routing work to a worker* as activity so the probe
   restarts (its new fence then lands after that work), while read-only queries stay non-bumping —
   otherwise a caller polling state restarts the probe forever.
6. **Bookkeeping created on first poll.** An accounting guard constructed inside the tracked future is
   never created if the task is aborted before its first poll, so the counter never decrements and a
   drain never completes. Fix: construct the guard at spawn and move ownership into the task.

Related, worth checking in the same sweep: a waiter map keyed by something narrower than the identity
it represents (a nonce without its requestor) — safe only while exactly one emitter exists.

## Deterministic tests, not load-dependent ones

An ordering defect is a race, and the hunt that finds it is not a regression test. Land a seam that
forces the interleaving:

- a `#[cfg(test)]` command in the hub's own enum (this repo's established style:
  `FailNextContentApplyRoute`, `InjectKeyhiveCompletionForTest`, `QuiescenceProbeBarrierForTest`),
  used to place the two messages in the wrong order or to close a mailbox at the exact moment;
- the real machinery around it (a real round, a real waiter, a real storage double that blocks inside
  the save) so the test exercises the production path, with only the ordering synthesized;
- state the **negative check**: revert the fix, show the test failing with the message that names the
  invariant, restore.

Do not paper over the race with retries, sleeps, or a longer timeout: those convert a correctness bug
into a slow success.
