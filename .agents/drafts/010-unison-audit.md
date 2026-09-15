# Checkout lifecycle audited against Unison

Reference:
[Unison manual](https://raw.githubusercontent.com/bcpierce00/unison/master/doc/unison-manual.tex),
especially update detection, reconciliation, propagation, and archive updates.
Executable counterexamples: `.agents/drafts/checkout-lifecycle.ts`. This is
design evidence, not a rewrite of ADR 010.

## What we can borrow

| Unison lifecycle                                                                                                    | Checkout analogue                                                                                                                                          | Demonstrated?                                                                                                                                                                                        |
| ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archive records the last successfully synchronized state per pair.                                                  | `common` binds Daybook's output ID/version to a filesystem path and confirmed bytes. Current reports do not replace it.                                    | Yes: edit accepted by Daybook does not advance `common` until filesystem projection completes.                                                                                                       |
| Scan each replica against its archive.                                                                              | FS observes paths against bound files; Daybook observes projected outputs against last common Daybook version.                                             | Partially: the toy scans one file and one output; no streaming ordered walks or per-backend report cache.                                                                                            |
| One-side change propagates; two different changes conflict; identical outcomes can be acknowledged without copying. | Checkout policy offers FS edits to Daybook first, then Daybook outputs to FS. Same-result edits are retried/acknowledged without another Daybook revision. | Yes for text, concurrent path moves, independent path/text changes, and same-result text edits.                                                                                                      |
| Check target still matches observation before replacing it; use atomic file replacement.                            | FS receiver checks old and destination bytes.                                                                                                              | Partially: toy map write is atomic; real FS needs temp+rename, re-stat/hash as required, and a safe nonrecursive removal protocol.                                                                   |
| Update archive only after propagation succeeds; interruption can be repaired by re-observing equal outcomes.        | Durable directional receipt for accepted FS→Daybook; write intent for pending Daybook→FS; advance common pair record only after observing target result.   | Toy simulates interruption after each receiver changed but before acknowledgement. Real persistence, no duplicates across process restart, and atomicity of ledger records are **not** demonstrated. |

## Where the analogy is unsafe

1. **Equality is not always semantic equality.** Unison can treat identical file
   bytes as synchronized. A Daybook lens output may have identical bytes under a
   different recipe/version, or two documents may independently produce the same
   bytes. A matching byte string may allow a receiver to avoid a copy, but
   cannot change document ownership or silently declare a new recipe already
   processed. The toy's same-result case is valid only for the _same bound
   output_ and a receiver that confirms it is already in the intended state.
2. **Identity across moves is not supplied by a path archive.** A snapshot of
   `/a` removed and `/c` added does not prove that `/c` is the same output. The
   toy injects optional move evidence; when absent it reports a missing bound
   output. A later design must establish ambiguity handling and stable binding
   semantics rather than make digest equality a universal move detector.
3. **The receiver may transform the offer.** Daybook ingestion can merge an
   independent facet change, canonicalize an output, or preserve a semantic
   conflict on a different branch. The generic pair cannot equate its original
   proposal with the receiver's result. It must observe the resulting Daybook
   projection before offering it to FS.
4. **Missing materialization differs from deletion.** A selected Daybook item
   whose blob or lens inputs are unavailable has not necessarily been removed.
   No virtual stub file is created; the checkout must not interpret unavailable
   source bytes as a deletion offer.
5. **Not every consumer needs a durable archive.** The WASI read-through mount
   holds a short-lived mapping from path to producer/version. On read it asks
   the producer for bytes at that version; there is no initial byte transfer and
   no ordinary FS→Daybook ingest cycle. The vtree entry needs an opaque
   production reference, not eager `text` bytes. If a WASI `stat` requires a
   length unknown before rendering, that operation may have to materialize
   lazily; it cannot fabricate a size.

## Concrete interface changes the demo suggests

- Replace eager `Entry { text }` with generic path/kind/opaque producer identity
  and optional verified byte digest or size; expose `open/read` or `produce` on
  the source. Materializing FS and mounting WASI are distinct receiver outcomes,
  both require truthful acknowledgement.
- Treat stable output key/version as a **capability required by the checkout
  policy's Daybook-producing side**, not by all backends. A path-only FS backend
  can still participate.
- Keep latest observations in a generic path-ordered vtree. Put durable
  last-common binding, directional ingest receipt, and write intent in a
  **checkout-specific ledger**; the generic bridge can store or transport opaque
  records, but it should not require WASI mounts to carry a checkout ledger.
- A crash test must distinguish: receiver not called; receiver applied but
  receipt missing; receipt persisted but projection not attempted; FS wrote but
  common base not advanced; and FS target changed after writing. A single
  `Applied` flag is insufficient for all phases.

## Next tests before claiming a design

Multi-output lens with partial deletion; a new unbound FS file; two identical
files and no move evidence; an accepted Daybook write whose resulting projection
differs from the proposed FS bytes; a versioned WASI file read after its
producer can no longer serve that version; failure during a multi-path move;
collision allocation between two output identities. Only then freeze the generic
rep and offer signatures.
