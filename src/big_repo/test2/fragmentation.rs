//! Tier 6 — Sedimentree fragmentation: deterministic boundary creation,
//! fragment storage verification, sync convergence, reload materialization.
//!
//! # Design
//!
//! `CountLeadingZeroBytes` (the default depth metric) makes any commit whose
//! first ID byte is `0x00` a boundary (~1/256 probability per commit).  The
//! test fixes the Automerge actor ID and writes a deterministic transaction
//! sequence until one change hash starts with `0x00`. The 2048-commit bound is
//! a guard against changes to the hash scheme, not a timing retry.
//!
//! # Coverage
//!
//! 1. **Fragment storage** — after identifying a boundary commit, verify that
//!    `Storage::load_fragment_metas` returns a non-empty fragment set for the
//!    document's sedimentree.
//! 2. **Sync convergence** — the peer receives the fragmented document and
//!    Tier-0 invariants (sedimentree parity, materialised-heads parity) hold.
//! 3. **Reload materialisation** — drop all handles, re-acquire from storage,
//!    and assert that the full Automerge document (including ancestors covered
//!    by the fragment boundary closure) is materialised.

use super::harness::{fixtures, heads, Pair};
use automerge::{transaction::Transactable, ChangeHash, ReadDoc, ScalarValue};
use future_form::Sendable;
use keyhive_core::access::Access;
use sedimentree_core::id::SedimentreeId;
use subduction_core::storage::traits::Storage;

/// Read a string value at `key` under ROOT.
async fn read_text(handle: &crate::BigDocHandle, key: &str) -> Option<String> {
    handle
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, key)
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(value) => match value.as_ref() {
                        ScalarValue::Str(value) => Some(value.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await
}

/// Write a key/value pair and return the resulting Automerge heads.
async fn write_and_get_heads(
    handle: &crate::BigDocHandle,
    key: &str,
    value: &str,
) -> crate::Res<Vec<ChangeHash>> {
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, key, value))
                .map(|_| doc.get_heads())
                .map_err(|err| crate::ferr!("failed tx: {err:?}"))
        })
        .await?
}

/// Write Automerge transactions until at least one change hash has its first
/// byte equal to `0x00` (making it a fragment boundary under `CountLeadingZeroBytes`).
/// Returns the count of transactions written and the first boundary head found.
///
/// The maximum number of attempts is bounded (2048) as a hard guard against an
/// infinite loop if the Automerge change hash scheme changes, but in practice
/// each attempt has a ~1/256 success probability.
async fn produce_boundary_commit(handle: &crate::BigDocHandle) -> crate::Res<(usize, ChangeHash)> {
    let mut attempt: usize = 0;
    loop {
        attempt += 1;
        if attempt > 2048 {
            return Err(crate::ferr!(
                "exceeded 2048 writes without producing a boundary hash \
                 (probability ~0.00033) — Automerge hash scheme may have changed"
            ));
        }
        let key = format!("b_{attempt}");
        let value = format!("v_{attempt}");
        let heads = write_and_get_heads(handle, &key, &value).await?;

        // The newest head is the one not present previously; in a linear
        // (non-forking) sequence it's the sole element.
        for head in &heads {
            if head.0[0] == 0 {
                return Ok((attempt, *head));
            }
        }
    }
}

/// Load fragment metadata from storage for a given document's sedimentree.
async fn load_fragment_metas(
    store: &crate::SqliteBigRepoStore,
    doc_id: crate::DocumentId,
) -> crate::Res<Vec<sedimentree_core::fragment::Fragment>> {
    let sed_id = SedimentreeId::new(doc_id.into_bytes());
    <crate::SqliteBigRepoStore as Storage<Sendable>>::load_fragment_metas(store, sed_id)
        .await
        .map_err(|e| crate::ferr!("failed loading fragment metas: {e}"))
}

// ─── Test ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_fragmentation_convergence() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();

    // ── 1. Boot pair and create doc ─────────────────────────────────────────
    let pair = Pair::boot(70, 71, "Owner", "Reader").await?;

    // Create initial document content before the boundary so reload verifies
    // that fragmented storage preserves the complete Automerge history.
    let mut initial = automerge::Automerge::new();
    initial.set_actor(automerge::ActorId::from([70_u8; 16]));
    initial
        .transact(|tx| tx.put(automerge::ROOT, "ancestor", "present"))
        .map_err(|err| crate::ferr!("failed initial ancestor: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // ── 2. Produce a boundary commit ────────────────────────────────────────
    let (_attempts, boundary_head) = produce_boundary_commit(&owner_doc).await?;
    tracing::info!(
        "produced boundary commit: head_first_byte={:#04x} head={:?}",
        boundary_head.0[0],
        boundary_head,
    );

    // Write a few more commits *after* the boundary so the fragment covers
    // real data and the peer sync exercises fragment transfer.
    for i in 0..8 {
        write_and_get_heads(&owner_doc, &format!("post_boundary_{i}"), "yes").await?;
    }

    // Fragment generation is asynchronous document publication, independent
    // of the transaction returning. Settle it before inspecting storage.
    pair.left().repo.wait_for_quiescence(None).await?;

    // ── 3. Verify fragment storage ─────────────────────────────────────────
    // The owner's store contains the document's sedimentree; load fragment
    // metadata to prove at least one fragment was persisted.
    let fragments = load_fragment_metas(&pair.left().store, doc_id).await?;
    assert!(
        !fragments.is_empty(),
        "at least one fragment must exist after a boundary commit, \
         got {} fragments for sedimentree {:?}",
        fragments.len(),
        SedimentreeId::new(doc_id.into_bytes()),
    );
    tracing::info!(
        "stored {} fragment(s); first fragment head={:?}",
        fragments.len(),
        fragments[0].head(),
    );

    // ── 4. Grant access and sync to peer ────────────────────────────────────
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(&pair, doc_id, &agent, Access::Read).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    // ── 5. Tier-0 invariants (sedimentree parity + materialized-heads parity) ─
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    // Content assertions: both sides see the same data.
    assert_eq!(
        read_text(&reader_doc, "ancestor").await.as_deref(),
        Some("present"),
        "ancestor content must survive fragment storage and sync",
    );
    assert_eq!(
        read_text(&reader_doc, &format!("b_{}", 1)).await.as_deref(),
        Some("v_1"),
        "first boundary-adjacent commit must be readable on reader",
    );

    // ── 6. Reload materialization from storage ──────────────────────────────
    // Drop all handles so the doc worker evicts the live bundle.
    drop(owner_doc);
    drop(reader_doc);

    // The reader re-acquires a handle from storage (no sync).  This exercises
    // `DocWorker2::load_doc_snapshot` which hydrates the tree from storage
    // (including fragments), runs `try_causal_decrypt`, and reassembles the
    // Automerge document.
    let reloaded = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;

    // Full content verification after reload.
    assert_eq!(
        read_text(&reloaded, "ancestor").await.as_deref(),
        Some("present"),
        "reloaded doc must contain ancestor data from before boundary",
    );
    for i in 0..8 {
        assert_eq!(
            read_text(&reloaded, &format!("post_boundary_{i}"))
                .await
                .as_deref(),
            Some("yes"),
            "reloaded doc must contain post-boundary commit {i}",
        );
    }

    // Also verify the owner side (which still has the doc worker loaded) can
    // reload without issues.
    let owner_reloaded = pair
        .left()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(
        read_text(&owner_reloaded, "ancestor").await.as_deref(),
        Some("present"),
        "owner reload must also preserve ancestor closure",
    );

    // ── 7. Final Tier-0 invariants after reload ────────────────────────────
    // The reloaded handles may be on different actual Automerge objects but
    // must agree on heads (materialized parity).
    heads::assert_materialized_parity_handles(
        pair.left().label,
        &owner_reloaded,
        pair.right().label,
        &reloaded,
    )
    .await?;

    drop(owner_reloaded);
    drop(reloaded);
    Ok(())
}
