# Header signature ordering counterexample

Status: pre-fix model counterexample confirmed and repaired by parent in Rust. Parent reported two real Ed25519 signatures (same scalar, different ExpandedSecretKey hash_prefix), both accepted by LaneHeader.verify/verify_strict; the Rust regression failed before and passed after moving header signature first in canonical_cmp. This worker did not execute Rust tests; it reread corrected source and migrated the model while retaining this historical evidence and a temporary executable pre-fix mutant.

Relevant source from the INITIAL register snapshot (`2DEC`), not the current corrected comparator:

1. `Representation::canonical_cmp`: binding, publisher, ciphertext, original header signature, representation signature.
2. `EncryptedRegister::join_lane`: same-version/same-semantic Current picks the least representation by that comparator; same-version conflicts combine available headers and choose two least distinct semantic IDs with minimum header signature variants.

Let all inputs be authenticated classes for writer1, sequence1, this record:

| Name | Semantic identity | Header signature rank | Representation binding rank |
|---|---:|---:|---:|
| cs | 10 | 1 | 2 |
| c | 10 | 2 | 1 |
| b | 20 | 2 | 1 |

`cs` and `c` contain exactly the same complete original-statement commitment and frontier. They differ only in a valid header-signature class and wrapper fields. Thus they have the same semantic identity. The signatures need not be equal merely because the writer/message are equal: `verify_strict` checks validity, not deterministic nonce derivation, and the public API accepts a generic signer. The parent confirmed this is a reachable class unless a concrete crypto proof contradicts it; no concrete Rust signature experiment was executed by this worker.

Order A:

- Receive cs -> Current(cs).
- Receive c -> Current(c), because binding1 < binding2. Smaller header signature1 is no longer retained.
- Receive b -> Poison `{semantic10/signature2, semantic20/signature2}`.

Order B:

- Receive cs -> Current(cs).
- Receive b -> Poison `{semantic10/signature1, semantic20/signature2}`.
- Receive c -> the poison retains signature1 for semantic10.

Both orders have the same genuine authenticated input set and the same poisoned logical lane, but different exact witness bytes. The independent full-history oracle expects signature1. The discrepancy means `join(join(cs,c),b) != join(cs,join(c,b))` under the modeled source comparator; a Current merge loses information that a future poison merge needs.

Observed TLC full-safety counterexample before addition of the fourth-conflict fixture: initial -> cs -> c -> b; 184 generated states, 89 distinct states, graph depth4 when violation was found; fp93, seed5510657950416982649. Invariants violating here are exact canonical evidence/history equivalence, not structural poison exclusion or replay sequence monotonicity. Other safety obligations remain separately checked rather than silently replacing the failed full contract.

Reproduce the historical fault with `npm run negative:register`: its pre-fix comparator mutant must fail signatureVariantFutureConflictTest and the full five-input fair-convergence property. Current corrected source/model should pass `npm run verify:register`, including mandatory full algebra and signature-conflict convergence, not merely the positive six-input recovery experiment.

The repair changes canonical order to header signature, binding, publisher, ciphertext, representation signature. Both Current and Poison now retain the same minimum signature variant. The model and independent order oracle were migrated only after a fresh source read; valid signature classes and all independent properties remain enabled. See results.md for actual post-migration outputs.
