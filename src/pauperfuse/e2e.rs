//! End-to-end checks of the cycle this crate exists for: a doc-backed rep and a
//! checkout converging, and then *stopping* — no echo, no re-render, no
//! re-hash (ADR 010 §2.3, §2.5).
//!
//! Everything here goes through [`reconcile`], the real pass, against the fake
//! doc backend in [`crate::test_support`] and a real filesystem. What it pins is
//! the property the design turns on: once a pass has run, both sides are quiet,
//! and the provenance of what moved survives on the side it moved to.
//!
//! The two directions are not the same pass, and the tests say so. A **checkout
//! update** (`source: doc`) transfers what the doc has. An **ingest**
//! (`source: checkout`) takes in what the checkout did to paths the doc already
//! covers — an edit is bytes the doc owns being replaced, which is a different
//! act from claiming a path.

use crate::backend::Backend;
use crate::bridge::{Outcome, reconcile};
use crate::delta::{Delta, DiffWalk};
use crate::entry::{Payload, Token};
use crate::fs::TokioFs;
use crate::interlude::*;
use crate::store::VtreeStore;
use crate::store::mem::MemVtreeStore;
use crate::test_support::{LensBackend, TestTree, path, scan};

/// A doc renders into a checkout, records what it wrote, and then has nothing
/// left to say in either direction.
#[tokio::test]
async fn a_doc_rep_materializes_once_and_then_stops_talking() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("notes/plan.md", b"# the plan\n");
    doc.edit("notes/2024/goals.md", b"goals\n");
    let disk = backend.id();

    // What the doc side reports is paths and recipes, and nothing has been
    // rendered: reporting a doc costs no renders.
    let reported = scan(&doc, &store).await?;
    assert_eq!(reported.len(), 2, "{reported:#?}");
    assert!(reported.iter().all(Delta::needs_transfer), "{reported:#?}");

    // Plan against the empty checkout and carry it out.
    let outcome = reconcile(&doc, &backend, &store).await?;
    assert_eq!(
        outcome.transferred, 4,
        "two files and the two directories they imply"
    );
    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.target_only, 0);
    assert_eq!(
        std::fs::read(tree.root().join("notes/plan.md"))?,
        b"# the plan\n"
    );

    // The fixed point in both directions: nothing left to transfer, and nothing
    // for the checkout to report.
    assert_eq!(reconcile(&doc, &backend, &store).await?, Outcome::default());
    let echoed = scan(&backend, &store).await?;
    assert!(
        echoed.is_empty(),
        "the checkout echoed the transfer: {echoed:#?}"
    );

    // The provenance survived the round trip: the disk side still knows which
    // doc, and which render of it, this file is.
    let recorded = store
        .entry(&disk, &path("notes/plan.md"))
        .await?
        .expect("the pass recorded what it wrote");
    match &recorded.payload {
        Payload::File { origin, content } => {
            assert_eq!(
                origin,
                &doc.identity("notes/plan.md"),
                "the doc's own identity, not a hash of the bytes"
            );
            assert_eq!(
                content.as_ref(),
                Some(&Token::blake3_of(b"# the plan\n")),
                "and what its bytes hashed to"
            );
        }
        other => panic!("expected the render recipe to survive, got {other:?}"),
    }
    assert_eq!(
        recorded.claim,
        Some(doc.owner()),
        "and which doc owns the path, which is what licenses its removal"
    );
    Ok(())
}

/// An edit to a rendered file is ingested: the doc takes the bytes a checkout
/// wrote to a path the doc already covers. The bytes are the doc's to begin
/// with, and the user is its owner in practice.
#[tokio::test]
async fn a_hand_edited_file_is_ingested_into_the_doc() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;
    assert!(scan(&backend, &store).await?.is_empty());

    // The user edits the file. The checkout reports exactly one change — the
    // file, not the directory it sits in.
    tree.write("plan.md", b"my own plan\n")?;
    let reported = scan(&backend, &store).await?;
    assert_eq!(reported.len(), 1, "{reported:#?}");
    assert_eq!(reported[0].path(), &path("plan.md"));

    // Ingest is the same pass with the direction swapped: the checkout is the
    // source, the doc is the target, and the target's `materialize` is where the
    // bytes become doc content.
    let outcome = reconcile(&backend, &doc, &store).await?;
    assert_eq!(outcome.transferred, 1, "the edit, and nothing else");
    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.target_only, 0);
    assert_eq!(
        doc.content("plan.md").as_deref(),
        Some(&b"my own plan\n"[..])
    );
    assert_eq!(doc.faceted(), vec!["plan.md".to_string()]);

    // And the loop is closed: the checkout has nothing new to report, and
    // another ingest pass has nothing to do.
    assert!(scan(&backend, &store).await?.is_empty());
    assert_eq!(reconcile(&backend, &doc, &store).await?, Outcome::default());

    // The doc's own report then says who these bytes belong to: the bridge
    // recorded the *source's* identity, which is all a transfer can know, and the
    // doc replaces it with its own and the claim that licences a later removal.
    let corrected = scan(&doc, &store).await?;
    assert_eq!(corrected.len(), 1, "{corrected:#?}");
    assert!(
        matches!(&corrected[0], Delta::Changed { to, .. } if to.claim.is_some()),
        "the doc re-asserts its identity and its ownership: {corrected:#?}"
    );

    // Which means the doc's identity for the path moved, so the checkout receives
    // the path again: a lens has to run to know what it produces, so a target
    // holding bytes from the *old* identity cannot be told they are still what
    // the new one makes. For this identity lens that is the same bytes arriving
    // twice; for a lens that canonicalizes, it is where a user's bytes become the
    // doc's rendering.
    let rendered = reconcile(&doc, &backend, &store).await?;
    assert_eq!(
        rendered.transferred, 1,
        "the doc named new bytes for the path"
    );
    assert_eq!(tree.read("plan.md")?, b"my own plan\n", "and they match");
    assert_eq!(reconcile(&doc, &backend, &store).await?, Outcome::default());
    assert!(scan(&backend, &store).await?.is_empty());
    Ok(())
}

/// Rendering is per path, not per doc: a doc edit re-renders what it touched.
/// A doc whose every path re-rendered on every change would make a large
/// checkout a full rewrite per keystroke (ADR 012 §2.
#[tokio::test]
async fn a_doc_edit_re_renders_only_the_path_it_touched() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("a.md", b"a1\n");
    doc.edit("b.md", b"b1\n");
    doc.edit("c.md", b"c1\n");

    scan(&doc, &store).await?;
    let first = reconcile(&doc, &backend, &store).await?;
    assert_eq!(first.transferred, 3);
    assert!(scan(&backend, &store).await?.is_empty());

    // One path changes upstream. The doc reports one delta, and the pass
    // transfers one path: the other two are neither re-rendered nor re-hashed.
    doc.edit("b.md", b"b2\n");
    let reported = scan(&doc, &store).await?;
    assert_eq!(reported.len(), 1, "{reported:#?}");
    assert_eq!(reported[0].path(), &path("b.md"));

    // The walk is exactly one disagreement long: a path where nothing differs is
    // not a delta at all, so the other two cost one row comparison and no more.
    let walk = DiffWalk::new(Arc::clone(&store), &backend.id(), &doc.id())
        .collect()
        .await?;
    assert_eq!(walk.len(), 1, "{walk:#?}");

    let outcome = reconcile(&doc, &backend, &store).await?;
    assert_eq!(outcome.transferred, 1, "only what the doc edited");
    assert_eq!(outcome.unchanged, 0, "and nothing else was even looked at");
    assert_eq!(std::fs::read(tree.root().join("b.md"))?, b"b2\n");
    assert_eq!(std::fs::read(tree.root().join("a.md"))?, b"a1\n");
    assert_eq!(std::fs::read(tree.root().join("c.md"))?, b"c1\n");
    assert!(scan(&backend, &store).await?.is_empty());
    Ok(())
}

/// A lens upgrade re-renders every path of that doc, even when the new lens
/// happens to produce the same bytes: the recipe is what says "these bytes came
/// from render N of this lens", and a reader cannot know in advance that N+1
/// agrees with N (ADR 012 §2).
#[tokio::test]
async fn a_lens_version_bump_re_renders_every_path_of_that_doc() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("a.md", b"a\n");
    doc.edit("b.md", b"b\n");
    doc.edit("c.md", b"c\n");

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;
    assert!(scan(&backend, &store).await?.is_empty());

    doc.bump_lens_ver();
    let reported = scan(&doc, &store).await?;
    assert_eq!(reported.len(), 3, "every path renders under the new lens");

    let outcome = reconcile(&doc, &backend, &store).await?;
    assert_eq!(outcome.transferred, 3);
    assert_eq!(outcome.unchanged, 0);
    assert_eq!(
        std::fs::read(tree.root().join("a.md"))?,
        b"a\n",
        "the identity lens renders the same bytes, and they are still rewritten"
    );
    assert!(scan(&backend, &store).await?.is_empty());
    Ok(())
}

/// Deleting a rendered file is the user's way of saying the doc's path should
/// go. The doc-side deletion is a move to trash (ADR 011 §4), so the bytes are
/// recoverable — and the file must *stay* deleted: a cycle that re-rendered it
/// would make a checkout impossible to prune.
#[tokio::test]
async fn deleting_a_rendered_file_trashes_the_face_and_stays_deleted() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");
    doc.edit("notes.md", b"notes\n");

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;
    assert!(scan(&backend, &store).await?.is_empty());

    // The user deletes one file. The checkout reports the removal.
    std::fs::remove_file(tree.root().join("plan.md"))?;
    let reported = scan(&backend, &store).await?;
    assert_eq!(reported.len(), 1, "{reported:#?}");
    assert!(
        matches!(reported[0], Delta::Removed { .. }),
        "{reported:#?}"
    );

    // Ingesting the removal is the doc moving the path to trash: the doc's rep
    // holds the entry, the checkout's does not, and the entry carries the lens
    // stamp that says a doc owned the path — which is the whole reason this
    // removal is licensed and a checkout's own file would not be.
    let outcome = reconcile(&backend, &doc, &store).await?;
    assert_eq!(outcome.removed, 1);
    assert_eq!(outcome.target_only, 0);
    assert_eq!(doc.content("plan.md"), None);
    assert_eq!(
        doc.trashed("plan.md").as_deref(),
        Some(&b"the plan\n"[..]),
        "a deletion is recoverable, not destructive"
    );
    assert_eq!(doc.faceted(), vec!["notes.md".to_string()]);

    // The other direction has nothing to re-render, and the deletion holds.
    assert_eq!(reconcile(&doc, &backend, &store).await?, Outcome::default());
    assert!(
        !tree.root().join("plan.md").exists(),
        "the pass undid the user's deletion"
    );
    assert!(tree.root().join("notes.md").exists());
    assert!(scan(&backend, &store).await?.is_empty());
    assert!(scan(&doc, &store).await?.is_empty());
    Ok(())
}

/// A file that reappears beside a trash is not re-adopted: a trash is a
/// decision, and importing is an explicit act (FDR 004 §3), not something a
/// reconcile pass does on its own initiative.
#[tokio::test]
async fn a_file_reappearing_beside_a_trash_is_not_re_adopted() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;

    std::fs::remove_file(tree.root().join("plan.md"))?;
    scan(&backend, &store).await?;
    assert_eq!(reconcile(&backend, &doc, &store).await?.removed, 1);

    // The user puts a file back at the same path.
    tree.write("plan.md", b"my own plan\n")?;
    scan(&backend, &store).await?;
    let outcome = reconcile(&backend, &doc, &store).await?;
    assert_eq!(
        outcome.transferred, 0,
        "the doc declines before any bytes are read"
    );
    assert_eq!(
        outcome.unchanged, 1,
        "and the offer is answered, not ignored"
    );
    assert_eq!(
        doc.content("plan.md"),
        None,
        "the doc declines what its trash already answered"
    );
    assert_eq!(doc.trashed("plan.md").as_deref(), Some(&b"the plan\n"[..]));
    assert!(
        tree.root().join("plan.md").exists(),
        "the user's file is their own until an import says otherwise"
    );
    Ok(())
}

/// A doc that drops a path takes it out of the checkout — and it can, because
/// the entry says a lens owned it. Without that, nothing in the two reps could
/// tell a doc's deletion from a file the checkout's user made themselves.
#[tokio::test]
async fn a_doc_that_drops_a_path_takes_it_out_of_the_checkout() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");
    doc.edit("notes.md", b"notes\n");
    let disk = backend.id();

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;
    assert!(scan(&backend, &store).await?.is_empty());
    assert!(tree.root().join("notes.md").exists());

    // The doc drops `notes.md`: its next report does not mention the path, so
    // the doc's own rep loses it.
    doc.drop_face("notes.md");
    let dropped = scan(&doc, &store).await?;
    assert_eq!(dropped.len(), 1, "the doc reported the drop: {dropped:#?}");

    let outcome = reconcile(&doc, &backend, &store).await?;
    assert_eq!(outcome.removed, 1);
    assert_eq!(outcome.target_only, 0);
    assert!(!tree.root().join("notes.md").exists());
    assert!(
        tree.root().join("plan.md").exists(),
        "and only that path went"
    );
    assert!(
        scan(&backend, &store).await?.is_empty(),
        "the checkout should agree with its rep after a removal"
    );
    assert_eq!(store.entry(&disk, &path("notes.md")).await?, None);
    Ok(())
}

/// A facet that fails validation bounces the ingest, and a bounce leaves both
/// sides exactly where they were: the source is never modified, and nothing is
/// recorded for the target (ADR 011 §5).
#[tokio::test]
async fn a_lens_that_cannot_represent_the_bytes_leaves_both_sides_alone() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");
    let doc_rep = doc.id();

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;

    // A write the doc's lens cannot represent, made by a user who has no idea
    // the doc is picky about it.
    doc.refusing(b"secret");
    tree.write("plan.md", b"secret plans\n")?;
    scan(&backend, &store).await?;
    let before = store
        .entry(&doc_rep, &path("plan.md"))
        .await?
        .expect("the rendered row is recorded");

    let error = reconcile(&backend, &doc, &store)
        .await
        .expect_err("the lens refused the bytes");
    assert!(error.to_string().contains("cannot represent"), "{error}");

    assert_eq!(
        doc.content("plan.md").as_deref(),
        Some(&b"the plan\n"[..]),
        "the doc kept what it had"
    );
    assert_eq!(
        tree.read("plan.md")?,
        b"secret plans\n",
        "and the checkout kept what its user wrote"
    );
    assert_eq!(
        store.entry(&doc_rep, &path("plan.md")).await?,
        Some(before),
        "a bounced pass records nothing for the target"
    );
    assert!(
        scan(&backend, &store).await?.is_empty(),
        "the checkout has nothing new to say"
    );
    Ok(())
}

/// A checkout whose records were lost does not re-copy what it can recognise.
///
/// This is the shape of the whole change: the core cannot tell whether two
/// identities stand for the same bytes, so it asks — and the answer can come from
/// the file itself, which no amount of comparing records in the core would find.
#[tokio::test]
async fn a_checkout_that_lost_its_records_recognises_its_own_bytes() -> Result<()> {
    let source_tree = TestTree::new()?;
    source_tree.write("notes/plan.md", b"# plan\n")?;
    let target_tree = TestTree::new()?;
    let source = TokioFs::new(source_tree.root()).with_id("a");
    let target = TokioFs::new(target_tree.root()).with_id("b");
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());

    scan(&source, &store).await?;
    reconcile(&source, &target, &store).await?;
    let named = store
        .entry(&source.id(), &path("notes/plan.md"))
        .await?
        .expect("the source's own identity");

    // The target's records go away — a store dropped, a tree copied from
    // somewhere else — and its files stay exactly where they were.
    store.drop_rep(&target.id()).await?;
    assert_eq!(
        store.entry(&target.id(), &path("notes/plan.md")).await?,
        None
    );

    let again = reconcile(&source, &target, &store).await?;
    assert_eq!(again.transferred, 0, "the bytes were already there");
    assert_eq!(again.unchanged, 2, "the file and the directory above it");
    assert_eq!(
        store
            .entry(&target.id(), &path("notes/plan.md"))
            .await?
            .expect("the record is rebuilt around what the checkout holds")
            .origin(),
        named.origin(),
        "under the identity the source named"
    );
    assert_eq!(
        reconcile(&source, &target, &store).await?,
        Outcome::default()
    );
    Ok(())
}

/// The same pass with a checkout on both sides. Nothing about it is doc-shaped:
/// a filesystem source and a filesystem target go through the same walk, the
/// same transfers and the same recording.
#[tokio::test]
async fn one_checkout_copies_another_and_keeps_up() -> Result<()> {
    let source_tree = TestTree::new()?;
    source_tree.write("notes/plan.md", b"# plan\n")?;
    source_tree.write("notes/2024/goals.md", b"goals\n")?;
    source_tree.mkdir("empty")?;
    source_tree.symlink("link.md", "notes/plan.md")?;
    let target_tree = TestTree::new()?;
    let source = TokioFs::new(source_tree.root()).with_id("a");
    let target = TokioFs::new(target_tree.root()).with_id("b");
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());

    scan(&source, &store).await?;
    let outcome = reconcile(&source, &target, &store).await?;
    assert_eq!(
        outcome.transferred, 6,
        "two files, the symlink, and three directories"
    );
    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.target_only, 0);

    // The copy is faithful, and the target's own scan agrees with what the pass
    // recorded: nothing to report and nothing to re-hash.
    assert_eq!(
        std::fs::read(target_tree.root().join("notes/plan.md"))?,
        b"# plan\n"
    );
    assert!(std::fs::symlink_metadata(target_tree.root().join("link.md"))?.is_symlink());
    assert!(target_tree.root().join("empty").is_dir());
    assert!(
        scan(&target, &store).await?.is_empty(),
        "the target's scan disagrees with what was recorded"
    );
    assert_eq!(
        reconcile(&source, &target, &store).await?,
        Outcome::default()
    );

    // A checkout's files are its user's, so nothing was linked: the two trees do
    // not share an inode, and an edit through one cannot land in the other.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let here = std::fs::metadata(source_tree.root().join("notes/plan.md"))?;
        let there = std::fs::metadata(target_tree.root().join("notes/plan.md"))?;
        assert_ne!(
            here.ino(),
            there.ino(),
            "fs content must never be aliased onto one inode"
        );
    }

    // An edit, an addition and a removed subtree on the source.
    source_tree.write("notes/plan.md", b"# plan v2\n")?;
    source_tree.write("notes/new.md", b"new\n")?;
    std::fs::remove_dir_all(source_tree.root().join("notes/2024"))?;
    scan(&source, &store).await?;

    let outcome = reconcile(&source, &target, &store).await?;
    assert_eq!(outcome.transferred, 2, "the edit and the addition");
    assert_eq!(
        outcome.target_only, 2,
        "the removed subtree: the target's paths are unstamped, so no pass deletes them"
    );
    assert_eq!(outcome.removed, 0);
    assert_eq!(
        std::fs::read(target_tree.root().join("notes/plan.md"))?,
        b"# plan v2\n"
    );
    assert!(
        target_tree.root().join("notes/2024/goals.md").exists(),
        "a checkout cannot prove another checkout's file was ever its own"
    );
    assert!(
        scan(&target, &store).await?.is_empty(),
        "leaving those paths alone must not make the target disagree with its rep"
    );

    // The target's own files are not the source's to delete: a pass answers for
    // what the source has, and says so rather than quietly pruning.
    let before = reconcile(&source, &target, &store).await?;
    target_tree.write("mine.md", b"mine\n")?;
    scan(&target, &store).await?;
    let after = reconcile(&source, &target, &store).await?;
    assert_eq!(
        after.target_only,
        before.target_only + 1,
        "the target's own file joins the paths a pass leaves alone"
    );
    assert_eq!(after.transferred, 0);
    assert_eq!(after.removed, 0);
    assert!(target_tree.root().join("mine.md").exists());
    Ok(())
}

/// A diff between a doc's rep and a checkout's rep is what the two passes plan
/// from, so the direction of the walk is worth pinning directly: `Added` is
/// "the right side has it", whatever the two sides are.
#[tokio::test]
async fn the_walk_is_left_to_right_whichever_side_is_a_doc() -> Result<()> {
    let tree = TestTree::new()?;
    let backend = TokioFs::new(tree.root());
    let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());
    let doc = LensBackend::new("doc");
    doc.edit("plan.md", b"the plan\n");
    let disk = backend.id();

    scan(&doc, &store).await?;
    reconcile(&doc, &backend, &store).await?;
    scan(&backend, &store).await?;

    // Both sides agree, so both orders are empty.
    assert!(
        DiffWalk::new(Arc::clone(&store), &disk, &doc.id())
            .collect()
            .await?
            .is_empty()
    );
    assert!(
        DiffWalk::new(Arc::clone(&store), &doc.id(), &disk)
            .collect()
            .await?
            .is_empty()
    );

    // A path the doc has and the checkout does not is `Added` when the doc is
    // the right side, and `Removed` when it is the left.
    let notes = path("notes.md");
    doc.edit("notes.md", b"notes\n");
    scan(&doc, &store).await?;
    let forward = DiffWalk::new(Arc::clone(&store), &disk, &doc.id())
        .collect()
        .await?;
    assert!(
        matches!(forward.as_slice(), [Delta::Added { path, .. }] if path == &notes),
        "{forward:#?}"
    );
    let backward = DiffWalk::new(Arc::clone(&store), &doc.id(), &disk)
        .collect()
        .await?;
    assert!(
        matches!(backward.as_slice(), [Delta::Removed { path, .. }] if path == &notes),
        "{backward:#?}"
    );
    Ok(())
}
