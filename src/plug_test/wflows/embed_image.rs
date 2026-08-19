use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use super::resolve_facet_write_target;
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let embedding_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::Embedding).to_string();
    let embedding_facet_tag =
        daybook_types::doc::FacetTag::from(WellKnownFacetTag::Embedding).to_string();
    let working_facet_target = resolve_facet_write_target(
        &args.primary_doc.facets,
        &args.primary_doc.tags,
        &embedding_facet_key,
        &embedding_facet_tag,
        "embedding",
    )?;

    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();
    let blob_facet_token = args
        .primary_doc
        .facets
        .iter()
        .find(|token| token.key() == blob_facet_key && token.rights().contains(FacetRights::READ))
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "blob facet key '{}' not found with read rights",
                blob_facet_key
            ))
        })?;

    let blob_raw = blob_facet_token
        .get()
        .map_err(|err| JobErrorX::Terminal(ferr!("access error reading blob facet: {err:?}")))?;
    let blob_json: daybook_types::doc::FacetRaw = serde_json::from_str(&blob_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing blob facet json: {err}")))?;
    let blob = match WellKnownFacet::from_json(blob_json, WellKnownFacetTag::Blob)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not blob")))?
    {
        WellKnownFacet::Blob(blob) => blob,
        _ => unreachable!("blob tag must parse as blob facet"),
    };

    if !blob.mime.starts_with("image/") {
        return Ok(());
    }

    let blob_embed_token = blob_facet_token.clone(None).map_err(|err| {
        JobErrorX::Terminal(ferr!("access error cloning blob facet token: {err:?}"))
    })?;
    let embed_result = mltools_embed::embed_image(blob_embed_token)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running embed-image: {err}")))?;

    let heads = am_utils_rs::parse_commit_heads(&args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("invalid heads from facet-routine: {err}")))?;
    let facet_key = daybook_types::doc::FacetKey::from(blob_facet_key.as_str());
    let facet_ref =
        daybook_types::url::build_facet_ref(daybook_types::url::FACET_SELF_DOC_ID, &facet_key)
            .map_err(|err| {
                JobErrorX::Terminal(err.wrap_err("error creating embedding facet_ref"))
            })?;
    let vector_bytes = embed_result
        .vector
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<u8>>();

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::Embedding(daybook_types::doc::Embedding {
                facet_ref: facet_ref.clone(),
                ref_heads: daybook_types::doc::ChangeHashSet(Arc::clone(&heads)),
                model_tag: embed_result.model_id.clone(),
                vector: vector_bytes.clone(),
                dim: embed_result.dimensions,
                dtype: daybook_types::doc::EmbeddingDtype::F32,
                compression: None,
            })
            .into();

        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_target.write(
            &new_facet,
            "access error updating image embedding facet",
            "access error creating image embedding facet",
        )?;
        Ok(Json(()))
    })?;

    Ok(())
}
