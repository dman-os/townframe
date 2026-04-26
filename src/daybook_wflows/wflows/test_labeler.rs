use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::WellKnownFacetTag;
    let args = facet_routine::get_args();

    let label_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::LabelGeneric).to_string();
    let working_facet_token = args
        .primary_doc
        .facets
        .iter()
        .find(|t| t.key() == label_facet_key && t.rights().contains(FacetRights::UPDATE))
        .ok_or_else(|| {
            JobErrorX::Terminal(ferr!(
                "labelGeneric facet token with update rights not found"
            ))
        })?;

    use daybook_types::doc::WellKnownFacet;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::LabelGeneric("test_label".into()).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_token
            .update(&new_facet)
            .map_err(|err| JobErrorX::Terminal(ferr!("access error updating facet: {err:?}")))?
            .map_err(|err| JobErrorX::Terminal(ferr!("error updating facet: {err:?}")))?;
        Ok(Json(()))
    })?;

    Ok(())
}
