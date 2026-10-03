//! End-to-end checkout slice over a real Daybook test node. Discovery through
//! the lazy process singletons is exercised by the trycmd suite; parallel tests
//! here never mutate ambient env/OnceCells and call the extracted internals.

use super::*;

use daybook_core::test_support::DaybookTestContext;
use daybook_types::doc::{
    AddDocArgs, Body, BranchPathBuf, FacetKey, Note, WellKnownFacet, WellKnownFacetTag,
};
use daybook_types::dpath::Dpath;
use daybook_types::url::build_facet_ref;

const NOTE_CONTENT: &str = "stored note body\nsecond line\n";

/// The checkout pipeline nests deeply through the Daybook runtime's mailbox
/// and drawer facade futures; the default 2 MiB tokio test stack overflows the
/// real poll chain, so the big test drives a runtime with larger stacks.
fn block_on_big_stack<F, T>(future: F) -> Res<T>
where
    F: Future<Output = Res<T>> + Send + 'static,
    T: Send + 'static,
{
    // The checkout pipeline nests deeply through the Daybook runtime's mailbox
    // and drawer facade futures; the default 2 MiB test stack overflows the
    // real poll chain before any await returns. Drive it from a dedicated
    // large-stack thread hosting the multi-thread runtime (test support uses
    // `block_in_place`, which requires a multi-thread scheduler).
    std::thread::Builder::new()
        .stack_size(96 << 20)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(96 << 20)
                .build()
                .expect("runtime build");
            runtime.block_on(future)
        })
        .expect("spawn big-stack test thread")
        .join()
        .expect("big-stack checkout test crashed")
}

async fn note_document(ctx: &DaybookTestContext) -> Res<String> {
    add_note_document(&ctx.drawer_repo).await
}

async fn add_note_document(drawer: &daybook_core::drawer::DrawerRepo) -> Res<String> {
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let note_url = build_facet_ref("self", &note_key)?;
    let args = AddDocArgs {
        branch_path: BranchPathBuf::from("main"),
        facets: [
            (
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                WellKnownFacet::TitleGeneric("Checkout".into()).into(),
            ),
            (
                note_key.clone(),
                WellKnownFacet::Note(Note {
                    mime: "text/plain".into(),
                    content: NOTE_CONTENT.into(),
                })
                .into(),
            ),
            (
                FacetKey::from(WellKnownFacetTag::Body),
                WellKnownFacet::Body(Body {
                    order: vec![note_url],
                })
                .into(),
            ),
            (
                Dpath::parse("/notes/hello.md")?.facet_key(),
                serde_json::json!({}),
            ),
        ]
        .into(),
        user_path: None,
    };
    drawer.add(args).await.map_err(eyre::Report::from)
}

async fn setup(test_name: &'static str) -> Res<DaybookTestContext> {
    let ctx = daybook_core::test_support::test_cx(test_name).await?;
    ensure_checkout_support(ctx.rt.plugs_repo.as_ref()).await?;
    ctx.rt.plugs_repo.ensure_core_plug().await?;
    Ok(ctx)
}

fn test_node(ctx: &DaybookTestContext) -> NodeHandle {
    NodeHandle {
        repo_root: ctx.rt.rcx.layout.repo_root.clone(),
        node_key: ctx.rt.rcx.iroh_public_key.clone(),
        drawer: ctx.rt.rcx.doc_drawer.document_id().to_string(),
    }
}

fn marker_json(root: &Path) -> Res<serde_json::Value> {
    Ok(serde_json::from_slice(&std::fs::read(root.join(MARKER))?)?)
}

#[test]
fn create_projects_note_and_status_reports_clean_modified_and_untracked() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_create_then_status").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        // Nested output directories start absent; projection creates them.
        let root = create(node.clone(), Arc::clone(&drawer), &base.path().join("sub"), document)
            .await
            .wrap_err("create checkout")?;

        // Marker: Ready, recorded node association, exact backend identity.
        let checkout = read_marker(&root).await?;
        assert!(matches!(checkout.state, State::Ready { .. }));
        assert_eq!(
            checkout.backend().0,
            format!("daybook:{}:{}", node.node_key, checkout.id)
        );
        assert!(checkout.node_path()?.is_absolute());
        let State::Ready { length, .. } = checkout.state else {
            unreachable!("asserted above")
        };
        assert_eq!(length as usize, NOTE_CONTENT.len());

        // The projection installed exactly the selected Note content.
        assert_eq!(
            tokio::fs::read(root.join("notes/hello.md")).await?,
            NOTE_CONTENT.as_bytes()
        );

        // Status from a nested descendant discovers the nearest marker.
        let deep = root.join("deep").join("nested");
        tokio::fs::create_dir_all(&deep).await?;
        let (found_root, found) = discover(&deep).await?;
        assert_eq!(found_root, root);
        assert_eq!(found, checkout);

        let lines = status(&root, &found, &drawer).await?;
        assert_eq!(lines, ["clean notes/hello.md"]);

        // A local edit and a stray file are visible without any ingestion or
        // publication.
        tokio::fs::write(root.join("notes/hello.md"), "locally edited").await?;
        tokio::fs::write(root.join("stray.md"), b"nobody tracks me").await?;
        let lines = status(&root, &found, &Arc::clone(&ctx.drawer_repo)).await?;
        assert_eq!(
            lines,
            ["modified notes/hello.md", "untracked stray.md"],
            "the tracked output is only modified; not also untracked"
        );

        // A second creation over the same directory is refused by the existing
        // marker; its identity stays untouched.
        let document = note_document(&ctx).await?;
        let before = marker_json(&root)?;
        let second = create(node, Arc::clone(&ctx.drawer_repo), &root, document).await;
        assert!(
            second.is_err(),
            "occupied checkout directory must be refused: {:#}",
            second.unwrap_err()
        );
        assert_eq!(marker_json(&root)?, before);

        ctx.stop().await?;
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn occupied_destination_refuses_before_any_visible_write() -> Res<()> {
    let ctx = setup("checkout_occupied_destination").await?;
    let document = note_document(&ctx).await?;
    let node = test_node(&ctx);
    let base = tempfile::tempdir()?;

    let directory = base.path().join("checkout");
    tokio::fs::create_dir_all(directory.join("notes")).await?;
    tokio::fs::write(directory.join("notes").join("hello.md"), b"user bytes").await?;

    let result = create(node, Arc::clone(&ctx.drawer_repo), &directory, document).await;
    assert!(result.is_err(), "occupied output must be refused");
    assert!(
        !directory.join(MARKER).exists(),
        "refusal must not leave a marker"
    );
    assert_eq!(
        tokio::fs::read(directory.join("notes").join("hello.md")).await?,
        b"user bytes"
    );
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_without_dpath_registration_is_refused_as_predating_checkout() -> Res<()> {
    // A node predating checkout support has an active core manifest without
    // the dpath facet. Every load re-seeds the fresh core manifest, so author
    // and enable the pre-checkout revision explicitly through the normal
    // validated plug path (removal is a 0.x minor bump, which skips the
    // command-preservation gate by design).
    let ctx = daybook_core::test_support::test_cx("checkout_old_node_refusal").await?;
    let plugs = std::sync::Arc::<daybook_core::plugs::PlugsRepo>::clone(&ctx.rt.plugs_repo);
    let mut old_core = daybook_core::plugs::system_plugs()
        .into_iter()
        .next()
        .ok_or_eyre("system_plugs is empty")?;
    old_core.version = "0.0.2".parse()?;
    let before = old_core.facets.len();
    old_core
        .facets
        .retain(|facet| facet.key_tag.to_string() != daybook_types::dpath::DPATH_FACET_TAG);
    assert_eq!(
        before - 1,
        old_core.facets.len(),
        "fixture removed exactly dpath"
    );
    plugs.add(old_core).await?;
    plugs.enable_known_plug("@daybook/core").await?;
    let result = ensure_checkout_support(plugs.as_ref()).await;
    let Err(error) = result else {
        panic!("old-node checkout support must be refused");
    };
    assert!(error.to_string().contains("predates checkout support"));
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn marker_projected_path_conflicting_with_marker_is_rejected() -> Res<()> {
    let ctx = setup("checkout_marker_reserved").await?;
    let document = note_document(&ctx).await?;
    let node = test_node(&ctx);
    let base = tempfile::tempdir()?;

    let created = create(
        node,
        Arc::clone(&ctx.drawer_repo),
        &base.path().join("ok"),
        document,
    )
    .await
    .wrap_err("create checkout")?;
    // A recorded projection whose output collides with the reserved marker file
    // cannot validate.
    let mut marker = read_marker(&created).await?;
    marker.projection.path = format!("{MARKER}/stolen");
    let error = marker.validate().expect_err("conflicting path rejected");
    assert!(error.to_string().contains(".daybook-checkout"), "{error:#}");
    ctx.stop().await?;
    Ok(())
}

#[test]
fn ingest_stages_edits_on_the_checkout_branch_and_never_touches_main() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_edits").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        let checkout = read_marker(&root).await?;
        let lines = status(&root, &checkout, &drawer).await?;
        assert_eq!(lines, ["clean notes/hello.md"]);

        // A local edit is observed, staged into the branch's Note, recorded as
        // a receipt, and never published: main stays at the recorded basis.
        tokio::fs::write(root.join("notes/hello.md"), "locally edited\n").await?;
        let lines = status(&root, &checkout, &drawer).await?;
        assert_eq!(lines, ["modified notes/hello.md"]);

        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let staged = read_marker(&root).await?;
        assert!(matches!(staged.state, State::Ready { blocked: None, .. }));
        assert_eq!(staged.receipts.len(), 1);
        assert_eq!(staged.receipts[0].path, "notes/hello.md");
        assert_eq!(
            staged.receipts[0].file,
            FileEvidence { length: "locally edited\n".len() as u64, digest: *blake3::hash(b"locally edited\n").as_bytes() }
        );

        let bundle = drawer
            .get_doc_bundle_at_branch(
                &staged.projection.document,
                BranchPath::new(&staged.branch),
                Some(vec![staged.projection.facet.clone()]),
            )
            .await?
            .expect("checkout branch still resolves");
        let note = validate_note(
            bundle
                .doc
                .facets
                .get(&staged.projection.facet)
                .expect("checkout branch Note expected"),
        )?;
        assert_eq!(note.content, "locally edited\n");

        // Never origin: the document's main heads are byte-identical to the
        // basis create recorded.
        let (_, main_heads) = drawer
            .get_with_heads(&staged.projection.document, BranchPath::new("main"), None)
            .await?
            .expect("document still on main");
        assert_eq!(
            am_utils_rs::serialize_commit_heads(&main_heads),
            staged.basis,
            "ingest must not move upstream main"
        );

        // Idempotent repeat: status reads ingested (confirmed), re-ingest is a
        // no-op and leaves the marker untouched.
        let lines = status(&root, &staged, &drawer).await?;
        assert_eq!(lines, ["ingested notes/hello.md"]);
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        assert_eq!(read_marker(&root).await?, staged);

        // Receipt heads match the live staged branch heads.
        assert_eq!(
            staged.receipts[0].branch_heads,
            am_utils_rs::serialize_commit_heads(&bundle.branch_heads)
        );

        ctx.stop().await?;
        Ok(())
    })
}

#[test]
fn ingest_blocks_on_non_utf8_preserves_bytes_and_clears_on_retry() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_non_utf8").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        let invalid = [0xC3u8, 0x28];
        tokio::fs::write(root.join("notes/hello.md"), &invalid).await?;

        let Err(error) = ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await else {
            panic!("non-UTF-8 edits must be refused");
        };
        assert!(error.to_string().contains("not valid UTF-8"), "{error:#}");

        // Blocking is durable; observation and the file stay available.
        let blocked = read_marker(&root).await?;
        assert!(
            matches!(&blocked.state, State::Ready { blocked: Some(_), .. }),
            "blocked ingest must be recorded in the marker"
        );
        assert_eq!(tokio::fs::read(root.join("notes/hello.md")).await?, invalid);
        let lines = status(&root, &blocked, &drawer).await?;
        assert!(lines[0].starts_with("blocked ingest "), "{lines:?}");
        assert_eq!(lines[1], "modified notes/hello.md");

        // Fixing the bytes and retrying resolves the block.
        tokio::fs::write(root.join("notes/hello.md"), "fixed\n").await?;
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let resolved = read_marker(&root).await?;
        assert!(matches!(resolved.state, State::Ready { blocked: None, .. }));
        let lines = status(&root, &resolved, &drawer).await?;
        assert_eq!(lines, ["ingested notes/hello.md"]);

        ctx.stop().await?;
        Ok(())
    })
}

#[test]
fn ingest_imports_untracked_files_only_through_explicit_allow() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_allow_import").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        tokio::fs::write(root.join("stray.md"), "imported body\n").await?;

        // Default refusal: untracked files are never auto-ingested.
        let checkout = read_marker(&root).await?;
        let lines = status(&root, &checkout, &drawer).await?;
        assert_eq!(lines, ["clean notes/hello.md", "untracked stray.md"]);
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        assert!(
            read_marker(&root).await?.imports.is_empty(),
            "default ingest must not import untracked files"
        );

        // Explicit per-path opt-in creates a new document identity, its local
        // checkout branch, and a binding + receipt record.
        let stray = std::path::absolute(root.join("stray.md"))?;
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, std::slice::from_ref(&stray)).await?;
        let imported = read_marker(&root).await?;
        assert_eq!(imported.imports.len(), 1);
        let import = &imported.imports[0];
        assert_eq!(import.projection.path, "stray.md");
        assert!(
            import.branch.starts_with("/tmp/checkout/"),
            "import branch must be a local /tmp branch"
        );

        let branch_ref = drawer
            .get_branch_ref(&import.projection.document, BranchPath::new(&import.branch))
            .await?
            .expect("import branch registered");
        assert_eq!(branch_ref.branch_kind, daybook_core::drawer::BranchKind::Local);
        assert_eq!(branch_ref.branch_doc_id.to_string(), import.branch_id);

        // The imported content is the imported bytes, on main too.
        let bundle = drawer
            .get_doc_bundle_at_branch(
                &import.projection.document,
                BranchPath::new(&import.branch),
                Some(vec![import.projection.facet.clone()]),
            )
            .await?
            .expect("import branch resolves");
        assert_eq!(
            am_utils_rs::serialize_commit_heads(&bundle.branch_heads),
            import.render_heads
        );
        let note = validate_note(
            bundle.doc.facets.get(&import.projection.facet).expect("imported Note expected"),
        )?;
        assert_eq!(note.content, "imported body\n");

        // The imported file is now tracked and clean; re-allowing is refused.
        let lines = status(&root, &imported, &drawer).await?;
        assert_eq!(lines, ["clean notes/hello.md", "clean stray.md"]);
        let mut current = read_marker(&root).await?;
        let Err(error) = ingest_checked(&root, &mut current, &drawer, &[stray]).await else {
            panic!("re-importing a tracked path must be refused");
        };
        assert!(error.to_string().contains("already tracked"), "{error:#}");

        // The primary document's main heads are untouched by the import.
        let (_, primary_main) = drawer
            .get_with_heads(&imported.projection.document, BranchPath::new("main"), None)
            .await?
            .expect("primary document on main");
        assert_eq!(am_utils_rs::serialize_commit_heads(&primary_main), imported.basis);

        ctx.stop().await?;
        Ok(())
    })
}

#[test]
fn ingest_reports_missing_files_without_deleting_anything() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_missing").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        tokio::fs::remove_file(root.join("notes/hello.md")).await?;

        let lines = status(&root, &read_marker(&root).await?, &drawer).await?;
        assert_eq!(lines, ["missing notes/hello.md"]);

        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let checkout = read_marker(&root).await?;
        // A missing file is an observation: no facet removal, no receipts, and
        // the Note survives on the branch untouched.
        assert!(checkout.receipts.is_empty());
        assert!(matches!(checkout.state, State::Ready { blocked: None, .. }));
        let lines = status(&root, &checkout, &drawer).await?;
        assert_eq!(lines, ["missing notes/hello.md"]);

        ctx.stop().await?;
        Ok(())
    })
}

/// Reads the bound Note facet directly off a binding's checkout branch.
async fn branch_note(
    drawer: &daybook_core::drawer::DrawerRepo,
    track_projection: &Projection,
    branch: &str,
) -> Res<daybook_types::doc::Note> {
    let bundle = drawer
        .get_doc_bundle_at_branch(
            &track_projection.document,
            BranchPath::new(branch),
            Some(vec![track_projection.facet.clone()]),
        )
        .await?
        .ok_or_eyre("checkout branch must resolve")?;
    validate_note(
        bundle
            .doc
            .facets
            .get(&track_projection.facet)
            .expect("Note facet expected on the checkout branch"),
    )
    .map_err(eyre::Report::from)
}

#[test]
fn ingest_stages_a_revert_of_restored_render_bytes() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_revert").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        let render = read_marker(&root).await?;
        assert_eq!(
            branch_note(&drawer, &render.projection, &render.branch).await?.content,
            NOTE_CONTENT,
            "a fresh checkout serves the acknowledged render content"
        );

        // Stage a divergent edit, then put the render bytes back on disk.
        tokio::fs::write(root.join("notes/hello.md"), "locally edited\n").await?;
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        tokio::fs::write(root.join("notes/hello.md"), NOTE_CONTENT).await?;

        // The bytes match the acknowledged render, but the branch carries staged
        // work: neither clean nor ingested — the next ingest must stage the revert.
        let diverged = read_marker(&root).await?;
        let lines = status(&root, &diverged, &drawer).await?;
        assert_eq!(lines, ["staged diverging notes/hello.md"]);

        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let reverted = read_marker(&root).await?;
        let note = branch_note(&drawer, &reverted.projection, &reverted.branch).await?;
        assert_eq!(note.content, NOTE_CONTENT, "the revert reaches the branch");
        let receipt = reverted
            .receipts
            .iter()
            .find(|receipt| receipt.path == "notes/hello.md")
            .expect("revert keeps a receipt");
        assert_eq!(
            receipt.file,
            FileEvidence {
                length: NOTE_CONTENT.len() as u64,
                digest: *blake3::hash(NOTE_CONTENT.as_bytes()).as_bytes()
            }
        );

        // Main is still byte-identical to the create basis across the revert.
        let (_, main_heads) = drawer
            .get_with_heads(&reverted.projection.document, BranchPath::new("main"), None)
            .await?
            .expect("document still on main");
        assert_eq!(am_utils_rs::serialize_commit_heads(&main_heads), reverted.basis);

        // The branch now holds the render content again while its heads moved:
        // that state reads as ingested, never as clean, because heads diverged.
        let lines = status(&root, &reverted, &drawer).await?;
        assert_eq!(lines, ["ingested notes/hello.md"]);

        ctx.stop().await?;
        Ok(())
    })
}

#[test]
fn ingest_recovers_a_rewound_receipt_through_recomputation() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_crash_window").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        tokio::fs::write(root.join("notes/hello.md"), "locally edited\n").await?;
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let staged = read_marker(&root).await?;
        let heads_before = staged.receipts[0].branch_heads.clone();

        // Simulate the crash window: the branch was staged but the marker never
        // recorded it. Rewind the receipts to recreate exactly that state.
        let mut rewound = read_marker(&root).await?;
        rewound.receipts = Vec::new();
        replace_marker(&root, &rewound).await?;
        let rewound = read_marker(&root).await?;

        // The loss is an observable, exact state: staged on the branch, receipt
        // missing — never masquerading as modified.
        let lines = status(&root, &rewound, &drawer).await?;
        assert_eq!(lines, ["ingested (unconfirmed) notes/hello.md"]);

        // Recovery is recomputation: the no-op rule against the live branch
        // confirms without staging anything again. Receipt heads must match the
        // pre-rewind heads byte for byte (no branch movement, no duplicate op).
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, &[]).await?;
        let recovered = read_marker(&root).await?;
        assert_eq!(recovered.receipts.len(), 1);
        assert_eq!(recovered.receipts[0].branch_heads, heads_before);
        assert_eq!(
            recovered.receipts[0].file,
            FileEvidence {
                length: "locally edited\n".len() as u64,
                digest: *blake3::hash(b"locally edited\n").as_bytes()
            }
        );
        let note = branch_note(&drawer, &recovered.projection, &recovered.branch).await?;
        assert_eq!(note.content, "locally edited\n");
        let lines = status(&root, &recovered, &drawer).await?;
        assert_eq!(lines, ["ingested notes/hello.md"]);

        ctx.stop().await?;
        Ok(())
    })
}

#[test]
fn claim_resolution_adopts_or_drops_and_never_duplicates_identity() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_claims").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        let stray = std::path::absolute(root.join("stray.md"))?;
        tokio::fs::write(&stray, "imported body\n").await?;

        // Simulate an interrupted import: claim written, add not completed.
        let mut claimed = read_marker(&root).await?;
        claimed.pending_imports.push("stray.md".into());
        replace_marker(&root, &claimed).await?;

        // While a claim exists, ingestion is blocked outright.
        let mut current = read_marker(&root).await?;
        let Err(error) = ingest_checked(&root, &mut current, &drawer, &[]).await else {
            panic!("an unresolved import claim must block ingest");
        };
        assert!(
            error.to_string().contains("unresolved import claim"),
            "{error:#}"
        );

        // Drop: the claim clears with no stale state, the file returns to
        // untracked, and no document identity was created.
        let mut claimed = read_marker(&root).await?;
        resolve_claims(&root, &mut claimed, &drawer, &[], &["stray.md".into()]).await?;
        let dropped = read_marker(&root).await?;
        assert!(dropped.pending_imports.is_empty(), "drop clears the claim");
        assert!(dropped.imports.is_empty(), "drop leaves no binding behind");
        // The refused ingest above also durably recorded its block, which status
        // reports until a successful ingest clears it.
        let lines = status(&root, &dropped, &drawer).await?;
        assert_eq!(lines.len(), 3, "{lines:?}");
        assert!(lines[0].starts_with("blocked ingest unresolved import claim for stray.md"), "{}", lines[0]);
        assert_eq!(&lines[1..], ["clean notes/hello.md", "untracked stray.md"]);

        // Adopt: the interrupted add actually completed upstream — i.e. a
        // document on main already claims the path through its dpath facet.
        let note_key = FacetKey::from(WellKnownFacetTag::Note);
        let note_url = build_facet_ref("self", &note_key)?;
        let adopted_doc = drawer
            .add(AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [
                    (note_key.clone(), WellKnownFacet::Note(Note {
                        mime: "text/plain".into(),
                        content: "imported body\n".into(),
                    }).into()),
                    (
                        FacetKey::from(WellKnownFacetTag::Body),
                        WellKnownFacet::Body(Body { order: vec![note_url] }).into(),
                    ),
                    (
                        Dpath::parse("/stray.md")?.facet_key(),
                        serde_json::json!({}),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await
            .map_err(eyre::Report::from)?;
        let mut claimed = read_marker(&root).await?;
        claimed.pending_imports.push("stray.md".into());
        replace_marker(&root, &claimed).await?;

        let mut claimed = read_marker(&root).await?;
        resolve_claims(
            &root,
            &mut claimed,
            &drawer,
            // CLI contract: --allow/--adopt arguments resolve against the caller's cwd.
            &[format!("{}={adopted_doc}", stray.display())],
            &[],
        )
        .await?;
        let adopted = read_marker(&root).await?;
        assert!(adopted.pending_imports.is_empty(), "adopt clears the claim");
        assert_eq!(adopted.imports.len(), 1, "exactly one binding was created");
        let import = &adopted.imports[0];
        assert_eq!(import.projection.document, adopted_doc);
        assert_eq!(import.projection.path, "stray.md");
        let branch_ref = drawer
            .get_branch_ref(&adopted_doc, BranchPath::new(&import.branch))
            .await?
            .expect("adopted branch registered");
        assert_eq!(branch_ref.branch_kind, daybook_core::drawer::BranchKind::Local);
        assert_eq!(branch_ref.branch_doc_id.to_string(), import.branch_id);
        let note = branch_note(&drawer, &import.projection, &import.branch).await?;
        assert_eq!(
            note.content,
            "imported body\n",
            "adopted branch serves the imported bytes"
        );
        let receipt = adopted
            .receipts
            .iter()
            .find(|receipt| receipt.path == "stray.md")
            .expect("the adopted binding carries a receipt");
        assert_eq!(
            receipt.branch_heads, import.render_heads,
            "the receipt pins the adopted fork heads"
        );

        // The negative the mechanism exists for: re-importing the adopted path
        // must not create a second document identity for the same bytes.
        let mut current = read_marker(&root).await?;
        let Err(error) = ingest_checked(&root, &mut current, &drawer, std::slice::from_ref(&stray))
        .await
        else {
            panic!("re-importing an adopted path must be refused");
        };
        assert!(error.to_string().contains("already tracked"), "{error:#}");
        assert_eq!(
            read_marker(&root).await?.imports.len(),
            1,
            "the refusal must not have grown bindings: no duplicate identity"
        );

        // And a claim on a claimed-but-unadopted path blocks --allow too: the
        // claim guard fires before any entry (allowed or edit) is classified.
        let mut claimed = read_marker(&root).await?;
        claimed.pending_imports.push("stray.md".into());
        replace_marker(&root, &claimed).await?;
        let mut current = read_marker(&root).await?;
        let Err(error) = ingest_checked(&root, &mut current, &drawer, std::slice::from_ref(&stray))
        .await
        else {
            panic!("--allow onto a claimed path must be refused");
        };
        assert!(
            error.to_string().contains("unresolved import claim for stray.md"),
            "{error:#}"
        );

        // Cleanup the second claim so the node's marker ends coherent.
        let mut claimed = read_marker(&root).await?;
        resolve_claims(&root, &mut claimed, &drawer, &[], &["stray.md".into()]).await?;
        assert!(read_marker(&root).await?.pending_imports.is_empty());

        ctx.stop().await?;
        Ok(())
    })
}

// ---- CLI grammar coverage (E fallback) ----

/// Resolves the built binary exactly as the trycmd suite does: the e2e
/// harness's cargo-metadata resolution does not work from in-crate tests.
fn cli_bin_path() -> Res<std::path::PathBuf> {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
        std::path::PathBuf::from,
    );
    Ok(target_dir.join("debug/daybook_cli"))
}

/// Drives the real binary. This is the pre-approved fallback for the checkout
/// trycmd surface. Bounds of this coverage (each bound is an upstream harness
/// limitation, recorded for the parent's post-merge fix lane):
/// 1. no CLI verb authors a projection-worthy document (touch is title-only;
///    ed is a whole-doc JSON editor over content-addressed facet keys), so a
///    blackbox create→edit→ingest→status file cannot be authored;
/// 2. test binaries pin the secret store to the in-memory mock
///    (secrets_rs under cfg(test)/test-support), so repos crossed between
///    the test process and the binary cannot load each other's identity
///    secrets.
///
/// Grammar surface reachable without a repo is asserted here; the repository
/// state machine is covered by the in-crate e2e tests above.
fn run_cli(args: &[&str]) -> Res<std::process::Output> {
    // repo env: absolute, not a repo, so DAYB_REPO_PATH can't drift with cwd.
    // the spawn cwd is neutral; repo resolution ignores cwd for absolute paths.
    std::process::Command::new(cli_bin_path()?)
        .args(args)
        .env("DAYB_REPO_PATH", "/tmp/daybook-cli-grammar-no-repo")
        // Mirror the trycmd suite: suppress success tracing so stdout assertions
        // only see the ingest/status output lines.
        .env("RUST_LOG", "error,wflow::kvstore=off")
        .env("NO_COLOR", "1")
        .output()
        .map_err(Into::into)
}


#[test]
fn checkout_cli_grammar_drives_the_real_binary_grammar_surface() -> Res<()> {
    block_on_big_stack(async {
        // No repo is opened: DAYB_REPO_PATH points at an absolute non-repo dir,
        // so every invocation takes the repo-resolution/refusal path. The checkout
        // dir exists (discover only requires a directory to start from) but has
        // no marker at any ancestor.
        tokio::fs::create_dir_all("/tmp/daybook-cli-grammar-no-repo/co").await?;
        let checkout_dir = "/tmp/daybook-cli-grammar-no-repo/co";

        // Grammar: the ratified ingest surface through the real binary.
        let output = run_cli(&["checkout", "--help"])?;
        assert!(output.status.success(), "{:?}\n{}", output.stderr, String::from_utf8_lossy(&output.stdout));
        let help = String::from_utf8_lossy(&output.stdout);
        for verb in ["create", "ingest", "status"] {
            assert!(help.contains(verb), "checkout --help must list {verb}\n{help}");
        }

        let output = run_cli(&["checkout", "ingest", "--help"])?;
        assert!(output.status.success(), "{:?}\n{}", output.stderr, String::from_utf8_lossy(&output.stdout));
        let help = String::from_utf8_lossy(&output.stdout);
        for flag in ["--allow", "--adopt-import-claim", "--drop-import-claim"] {
            assert!(help.contains(flag), "ingest --help must list {flag}\n{help}");
        }
        // The adopt spelling parses one PATH=DOC value, not two positional values.
        assert!(
            help.contains("--adopt-import-claim <PATH=DOC>"),
            "the PATH=DOC spelling is the ratified grammar\n{help}"
        );

        // An absent repo refuses every checkout verb with a diagnostic and a
        // failure exit. No marker exists at any ancestor of the dir, so both
        // verbs fail in discovery before any repo is opened.
        let output = run_cli(&["checkout", "status", checkout_dir])?;
        assert!(!output.status.success(), "status outside a checkout must fail");
        let report = format!("{}{}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
        assert!(report.contains("no checkout marker found"), "{report}");

        // Ingest resolves the repo (lazy singletons for the plug check) before
        // discovery, so its refusal names the missing repo instead.
        let output = run_cli(&["checkout", "ingest", checkout_dir])?;
        assert!(!output.status.success(), "ingest outside a repo must fail");
        let report = format!("{}{}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
        assert!(report.contains("repo not initialized"), "{report}");

        Ok(())
    })
}

#[test]
fn batch_gate_refuses_everything_when_one_entry_fails_to_prepare() -> Res<()> {
    block_on_big_stack(async {
        let ctx = setup("checkout_ingest_batch_gate").await?;
        let document = note_document(&ctx).await?;
        let node = test_node(&ctx);
        let drawer = Arc::clone(&ctx.drawer_repo);
        let base = tempfile::tempdir()?;

        let root = create(node, Arc::clone(&drawer), &base.path().join("co"), document)
            .await
            .wrap_err("create checkout")?;
        let valid = std::path::absolute(root.join("stray.md"))?;
        tokio::fs::write(&valid, b"\xff\xfe not utf-8").await?;
        tokio::fs::write(root.join("notes/hello.md"), "locally edited\n").await?;

        // One unprepared --allow entry refuses the whole batch: the valid edit
        // is NOT staged either, and the block is recorded durably.
        let before = read_marker(&root).await?;
        let Err(error) =
            ingest_checked(&root, &mut read_marker(&root).await?, &drawer, std::slice::from_ref(&valid)).await
        else {
            panic!("a single unprepared entry must refuse the whole batch");
        };
        assert!(error.to_string().contains("not valid UTF-8"), "{error:#}");
        let gated = read_marker(&root).await?;
        assert!(gated.receipts.is_empty(), "no receipts: nothing staged");
        assert!(gated.imports.is_empty(), "no imports: nothing staged");
        assert!(gated.pending_imports.is_empty());
        assert!(
            matches!(gated.state, State::Ready { blocked: Some(_), .. }),
            "the refusal is recorded as a durable block"
        );

        let note = branch_note(&drawer, &before.projection, &before.branch).await?;
        assert_eq!(note.content, NOTE_CONTENT, "the branch held the render content");

        // Preparing the input and retrying clears the block and stages both.
        tokio::fs::write(&valid, "fixed stray\n").await?;
        ingest_checked(&root, &mut read_marker(&root).await?, &drawer, std::slice::from_ref(&valid)).await?;
        let resolved = read_marker(&root).await?;
        assert!(matches!(resolved.state, State::Ready { blocked: None, .. }));
        assert_eq!(resolved.receipts.len(), 2, "the retried batch stages both paths");
        assert_eq!(resolved.imports.len(), 1);
        let lines = status(&root, &resolved, &drawer).await?;
        assert_eq!(lines, ["ingested notes/hello.md", "clean stray.md"]);

        ctx.stop().await?;
        Ok(())
    })
}
