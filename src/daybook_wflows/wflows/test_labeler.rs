use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    let args = facet_routine::get_args();

    // Find the working facet token (the one with write access matching facet_key)
    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &args.facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "working facet key '{}' not found in rw_facet_tokens",
                args.facet_key
            ))
        })?;

    // Extract text content for LLM
    // Use root types since Doc uses root types (not WIT types)
    use daybook_types::doc::WellKnownFacet;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::LabelGeneric("test_label".into()).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .wrap_err("error updating facet")
            .map_err(JobErrorX::Terminal)?;
        Ok(Json(()))
    })?;

    Ok(())
}
