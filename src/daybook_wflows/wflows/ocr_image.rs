use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use super::resolve_facet_write_target;
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use crate::wit::townframe::daybook::mltools_ocr;
    use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

    let args = facet_routine::get_args();

    let note_facet_key = daybook_types::doc::FacetKey::from(WellKnownFacetTag::Note).to_string();
    let note_facet_tag = daybook_types::doc::FacetTag::from(WellKnownFacetTag::Note).to_string();
    let working_facet_target = resolve_facet_write_target(
        &args.primary_doc.facets,
        &args.primary_doc.tags,
        &note_facet_key,
        &note_facet_tag,
        "note",
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

    let blob_ocr_token = blob_facet_token.clone(None).map_err(|err| {
        JobErrorX::Terminal(ferr!("access error cloning blob facet token: {err:?}"))
    })?;
    let ocr_result = mltools_ocr::ocr_image(blob_ocr_token)
        .map_err(|err| JobErrorX::Terminal(ferr!("error running OCR: {err}")))?;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::Note(daybook_types::doc::Note {
                mime: "text/plain".to_string(),
                content: ocr_result.text.clone(),
            })
            .into();

        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_target.write(
            &new_facet,
            "access error updating note with OCR result",
            "access error creating note with OCR result",
        )?;

        Ok(Json(()))
    })?;

    Ok(())
}
