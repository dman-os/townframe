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
    ctx.drawer_repo.add(args).await.map_err(eyre::Report::from)
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
        let root = create(node.clone(), drawer, &base.path().join("sub"), document)
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

        let lines = status(&root, &found).await?;
        assert_eq!(lines, ["clean notes/hello.md"]);

        // A local edit and a stray file are visible without any ingestion or
        // publication.
        tokio::fs::write(root.join("notes/hello.md"), "locally edited").await?;
        tokio::fs::write(root.join("stray.md"), b"nobody tracks me").await?;
        let lines = status(&root, &found).await?;
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
    let plugs = ctx.rt.plugs_repo.clone();
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
