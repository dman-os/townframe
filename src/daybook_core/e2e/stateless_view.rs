use crate::interlude::*;

use daybook_types::doc::{
    AddDocArgs, FacetKey, FacetRaw, FacetTag, WellKnownFacet, WellKnownFacetTag,
};
use daybook_types::manifest::ViewRef;
use daybook_types::view::{ViewActionV1, ViewNodeKindV1, ViewSpec, ViewSpecV1};
use std::path::PathBuf;

async fn import_dayledger_oci(test_cx: &crate::test_support::DaybookTestContext) -> Res<()> {
    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join("@daybook/dayledger");
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo x build-plug-oci --plug-root ./src/plug_dayledger",
        artifact_path.display()
    );
    test_cx
        .rt
        .plugs_repo
        .import_from_oci_layout(&artifact_path, crate::plugs::OciImportOptions::default())
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn render_plug_test_stateless_view() -> Res<()> {
    let test_cx = crate::test_support::test_cx("render_plug_test_stateless_view").await?;
    crate::test_support::import_test_plug_oci(&test_cx).await?;

    let doc_id = test_cx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::LabelGeneric),
                FacetRaw::from(WellKnownFacet::LabelGeneric("seed".into())),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let rendered = test_cx
        .rt
        .render_facet_view(
            &doc_id,
            &daybook_types::doc::BranchPathBuf::from("main"),
            &FacetKey::from(WellKnownFacetTag::LabelGeneric),
            Some(ViewRef {
                plug_id: Some("@daybook/test".into()),
                view_key: "sample-summary-card".into(),
            }),
            None,
        )
        .await?;

    assert_eq!(rendered.plug_id, "@daybook/test");
    assert_eq!(rendered.view_key, "sample-summary-card");
    assert_eq!(rendered.plugin_state_json, None);

    let parsed: ViewSpec = serde_json::from_str(&rendered.view_json)?;
    let ViewSpec::V1(ViewSpecV1 { root }) = parsed;
    assert_eq!(root.id.0, "root");
    match root.kind {
        ViewNodeKindV1::Card(card) => {
            assert_eq!(card.title.as_deref(), Some("Sample summary"));
            assert_eq!(card.children.len(), 2);
            match &card.children[0].kind {
                ViewNodeKindV1::Markdown(markdown) => {
                    assert!(markdown.markdown.contains("plug_test"));
                }
                other => panic!("expected markdown child, got {other:?}"),
            }
            match &card.children[1].kind {
                ViewNodeKindV1::Button(button) => {
                    assert_eq!(button.label, "Emit event");
                    assert_eq!(card.children[1].events.len(), 1);
                    assert!(matches!(
                        &card.children[1].events[0].action,
                        ViewActionV1::Emit(_)
                    ));
                }
                other => panic!("expected button child, got {other:?}"),
            }
        }
        other => panic!("expected card root, got {other:?}"),
    }

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn render_dayledger_ledger_meta_stateless_view() -> Res<()> {
    let test_cx =
        crate::test_support::test_cx("render_dayledger_ledger_meta_stateless_view").await?;
    crate::test_support::import_test_plug_oci(&test_cx).await?;
    import_dayledger_oci(&test_cx).await?;

    let ledger_meta_key = FacetKey::from(FacetTag::from("org.example.dayledger.meta"));
    let doc_id = test_cx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: [(
                ledger_meta_key.clone(),
                serde_json::json!({
                    "ledgerId": "ledger-1",
                    "title": "Ledger Overview",
                    "journalCommodity": "USD",
                    "accountRefs": [
                        "db+facet:///doc/assets/main",
                        "db+facet:///doc/income/main"
                    ],
                    "transactionRefs": [
                        "db+facet:///doc/txn-1/main"
                    ]
                }),
            )]
            .into(),
            user_path: None,
        })
        .await?;

    let rendered = test_cx
        .rt
        .render_facet_view(
            &doc_id,
            &daybook_types::doc::BranchPathBuf::from("main"),
            &ledger_meta_key,
            None,
            None,
        )
        .await?;

    assert_eq!(rendered.plug_id, "@daybook/dayledger");
    assert_eq!(rendered.view_key, "ledger-meta");
    assert_eq!(rendered.plugin_state_json, None);

    let parsed: ViewSpec = serde_json::from_str(&rendered.view_json)?;
    let ViewSpec::V1(ViewSpecV1 { root }) = parsed;
    assert_eq!(root.id.0, "root");
    match root.kind {
        ViewNodeKindV1::Card(card) => {
            assert_eq!(card.title.as_deref(), Some("Ledger Overview"));
            assert_eq!(card.children.len(), 3);
            match &card.children[0].kind {
                ViewNodeKindV1::Section(section) => {
                    assert_eq!(section.title.as_deref(), Some("Summary"));
                    assert_eq!(section.children.len(), 4);
                }
                other => panic!("expected summary section, got {other:?}"),
            }
        }
        other => panic!("expected card root, got {other:?}"),
    }

    test_cx.stop().await?;
    Ok(())
}
