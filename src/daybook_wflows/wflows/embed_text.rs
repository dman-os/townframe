use super::super::*;
use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_embed;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let working_facet_token =
        tuple_list_get(&args.rw_facet_tokens, &args.facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_facet_token =
        tuple_list_get(&args.ro_facet_tokens, &note_facet_key).ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "note facet key '{}' not found in ro_facet_tokens",
                note_facet_key
            ))
        })?;

    let current_facet_raw = note_facet_token.get();

    let current_facet_json: daybook_types::doc::FacetRaw = serde_json::from_str(&current_facet_raw)
        .map_err(|err| JobErrorX::Terminal(ferr!("error parsing working facet json: {err}")))?;

    let current_note = WellKnownFacet::from_json(current_facet_json, WellKnownFacetTag::Note)
        .map_err(|err| JobErrorX::Terminal(err.wrap_err("input facet is not a note facet")))?;
    let WellKnownFacet::Note(note) = current_note else {
        return Err(JobErrorX::Terminal(ferr!("input facet is not note")));
    };

    // FIXME: put this in an effect
    let embed_result = mltools_embed::embed_text(&note.content)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running embed-text: {err}")))?;
    let heads = am_utils_rs::parse_commit_heads(&args.heads)
        .map_err(|err| JobErrorX::Terminal(ferr!("invalid heads from facet-routine: {err}")))?;
    let facet_key = daybook_types::doc::FacetKey::from(note_facet_key.as_str());
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
            .wrap_err("error updating embedding facet")
            .map_err(JobErrorX::Terminal)?;

        Ok(Json(()))
    })?;

    Ok(())
}
