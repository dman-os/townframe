use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag};
use daybook_types::manifest::ViewRef;
use daybook_types::view::{ViewActionV1, ViewNodeKindV1, ViewSpec, ViewSpecV1};

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
