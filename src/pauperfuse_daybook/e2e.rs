use crate::{Daybook, Error, Projection};
use daybook_core::drawer::DrawerRepo;
use daybook_core::test_support::test_cx;
use daybook_types::doc::{
    AddDocArgs, Body, BranchPath, BranchPathBuf, DocId, DocPatch, FacetKey, FacetRaw, Note,
    WellKnownFacet, WellKnownFacetTag,
};
use daybook_types::dpath::Dpath;
use daybook_types::url::{FACET_SELF_DOC_ID, build_facet_ref};
use pauperfuse::backends::Producer;
use pauperfuse::backends::{BackendId, BackendTree, ByteReader, Description, OutputVersion};
use std::collections::HashMap;
use std::sync::Arc;
use utils_rs::prelude::{Res, eyre};

type TestResult<T = ()> = Res<T>;

fn note_facets(content: &str) -> TestResult<HashMap<FacetKey, FacetRaw>> {
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let body = WellKnownFacet::Body(Body {
        order: vec![build_facet_ref(FACET_SELF_DOC_ID, &note_key)?],
    });
    Ok([
        (
            Dpath::parse("/notes/hello.md")?.facet_key(),
            serde_json::json!({}),
        ),
        (FacetKey::from(WellKnownFacetTag::Body), body.into()),
        (
            note_key,
            WellKnownFacet::Note(Note {
                mime: "text/plain".into(),
                content: content.into(),
            })
            .into(),
        ),
    ]
    .into_iter()
    .collect())
}

async fn add_note(drawer: &DrawerRepo, content: &str) -> TestResult<DocId> {
    Ok(drawer
        .add(AddDocArgs {
            branch_path: BranchPathBuf::from("main"),
            facets: note_facets(content)?,
            user_path: None,
        })
        .await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn select_claims_reads_a_real_drawer_document_and_records_provenance() -> TestResult {
    let context = test_cx("pauperfuse_daybook_select_claims_real_drawer").await?;
    let document = add_note(&context.drawer_repo, "first version\n").await?;

    let selected = Projection::select_claims(&context.drawer_repo, document.clone()).await?;
    assert_eq!(selected.document, document);
    assert!(!selected.heads.is_empty());

    let projected = selected.projected();
    assert_eq!(projected.len(), 1);
    let (_, projection, recipe) = &projected[0];
    assert_eq!(projection.path, "notes/hello.md");
    assert_eq!(projection.facet, FacetKey::from(WellKnownFacetTag::Note));
    assert_eq!(recipe.affected_inputs.len(), 1);
    assert_eq!(recipe.affected_inputs[0].document, document);
    assert_eq!(recipe.affected_inputs[0].facet, projection.facet);
    assert_eq!(
        recipe.affected_inputs[0].heads,
        am_utils_rs::serialize_commit_heads(&selected.heads)
    );
    assert_eq!(recipe.context_inputs.len(), 1);
    assert_eq!(
        recipe.context_inputs[0].facet,
        FacetKey::from(WellKnownFacetTag::Body)
    );
    assert_eq!(
        recipe.context_inputs[0].heads,
        recipe.affected_inputs[0].heads
    );

    context.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn daybook_reads_the_recorded_drawer_version_and_rejects_mismatched_heads() -> TestResult {
    let context = test_cx("pauperfuse_daybook_exact_drawer_version").await?;
    let document = add_note(&context.drawer_repo, "first version\n").await?;
    let selected = Projection::select_claims(&context.drawer_repo, document.clone()).await?;
    let (_, projection, _) = selected
        .projected()
        .into_iter()
        .next()
        .ok_or_else(|| eyre::eyre!("test document was not projected"))?;
    let producer = Daybook::new(
        Arc::clone(&context.drawer_repo),
        BackendId("pauperfuse-daybook-test".into()),
        projection,
        "main".into(),
        selected.heads.clone(),
    );

    let mut observation = producer.observe().await?;
    let mut source = None;
    while let Some(entry) = observation.next_entry().await? {
        if entry.path.to_string() == "notes/hello.md" {
            let Description::File { source: found, .. } = entry.description else {
                return Err(eyre::eyre!("projected note was not a file"));
            };
            source = Some(found);
        }
    }
    let source = source.ok_or_else(|| eyre::eyre!("projected note was absent"))?;

    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    context
        .drawer_repo
        .update_at_heads(
            DocPatch {
                id: document.clone(),
                facets_set: [(
                    note_key,
                    WellKnownFacet::Note(Note {
                        mime: "text/plain".into(),
                        content: "second version\n".into(),
                    })
                    .into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            BranchPath::new("main"),
            None,
        )
        .await?;
    let (_, current_heads) = context
        .drawer_repo
        .get_with_heads(&document, BranchPath::new("main"), None)
        .await?
        .ok_or_else(|| eyre::eyre!("updated document was absent"))?;
    assert_ne!(&*current_heads, &*selected.heads);

    let mut reader = producer.open(&source.output).await?;
    let mut bytes = [0; 64];
    let length = reader.read_at(0, &mut bytes).await?;
    assert_eq!(&bytes[..length], b"first version\n");

    let mismatched = OutputVersion {
        output: source.output.output.clone(),
        version: serde_json::to_vec(&am_utils_rs::serialize_commit_heads(&current_heads))?,
    };
    let error = match producer.open(&mismatched).await {
        Ok(_) => return Err(eyre::eyre!("mismatched heads were unexpectedly accepted")),
        Err(error) => error,
    };
    assert!(
        matches!(&error, Error::Unavailable(message) if message.contains("recorded recipe heads")),
        "unexpected mismatched-head error: {error}"
    );

    context.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn select_claims_reports_a_missing_drawer_document() -> TestResult {
    let context = test_cx("pauperfuse_daybook_missing_drawer_document").await?;
    let error = Projection::select_claims(
        &context.drawer_repo,
        "missing-pauperfuse-daybook-document".into(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&error, Error::Unavailable(message) if message.contains("missing-pauperfuse-daybook-document")),
        "unexpected missing-document error: {error}"
    );
    context.stop().await?;
    Ok(())
}
