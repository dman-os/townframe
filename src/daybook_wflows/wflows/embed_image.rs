use super::super::*;
use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let mut args = facet_routine::get_args();

    let working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let blob_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Blob).to_string();
    let mut ro_facet_tokens = std::mem::take(&mut args.ro_facet_tokens);
    let blob_facet_token =
        tuple_list_take(&mut ro_facet_tokens, &blob_facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "blob facet key '{}' not found in ro_facet_tokens",
                blob_facet_key
            ))
        })?;

    let blob_raw = blob_facet_token.get();
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

    let embed_result = mltools_embed::embed_image(blob_facet_token)
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
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating image embedding facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}
