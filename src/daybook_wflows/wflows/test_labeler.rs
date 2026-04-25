use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::WellKnownFacetTag;
    let args = facet_routine::get_args();

    let label_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::LabelGeneric).to_string();
    let working_facet_token = args
        .rw_facet_tokens
        .iter()
        .find(|(key, _)| key == &label_facet_key)
        .map(|(_, token)| token)
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "labelGeneric facet token not found in rw_facet_tokens"
            ))
        })?;

    // This test workflow writes a hardcoded label and does not read doc content.
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
