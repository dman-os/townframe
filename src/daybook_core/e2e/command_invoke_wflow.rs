use crate::interlude::*;
use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_command_invoke_success_reply() -> Res<()> {
    let test_cx = crate::test_support::test_cx("command_invoke").await?;
    crate::test_support::import_test_plug_oci(&test_cx).await?;

    let success_doc_id = test_cx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                FacetKey::from(WellKnownFacetTag::LabelGeneric),
                FacetRaw::from(WellKnownFacet::LabelGeneric("seed-success".into())),
            )]
            .into(),
            user_path: None,
        })
        .await?;
    let (_doc, success_heads) = test_cx
        .drawer_repo
        .get_with_heads(
            &success_doc_id,
            &daybook_types::doc::BranchPath::from("main"),
            None,
        )
        .await?
        .ok_or_eyre("success doc not found after add")?;

    let success_dispatch_id = test_cx
        .rt
        .dispatch(
            "@daybook/test",
            "invoke-child-success",
            crate::rt::DispatchArgs::DocRoutine {
                doc_id: success_doc_id.clone(),
                branch_path: daybook_types::doc::BranchPath::from("main"),
                heads: success_heads,
                changed_facet_keys: vec![],
                wflow_args_json: None,
            },
        )
        .await?;
    test_cx
        .rt
        .wait_for_dispatch_end(&success_dispatch_id, std::time::Duration::from_secs(120))
        .await?;

    let success_dispatch = test_cx
        .dispatch_repo
        .get_any(&success_dispatch_id)
        .await
        .ok_or_eyre("missing success dispatch after completion")?;
    assert!(matches!(
        success_dispatch.status,
        crate::rt::dispatch::DispatchStatus::Succeeded
    ));

    test_cx.stop().await?;
    Ok(())
}
