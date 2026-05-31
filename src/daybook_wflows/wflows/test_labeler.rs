use crate::interlude::*;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

pub fn run(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
    use super::resolve_facet_write_target;
    use crate::wit::townframe::daybook::facet_routine;
    use daybook_types::doc::WellKnownFacetTag;
    let args = facet_routine::get_args();

    let label_facet_key =
        daybook_types::doc::FacetKey::from(WellKnownFacetTag::LabelGeneric).to_string();
    let label_facet_tag =
        daybook_types::doc::FacetTag::from(WellKnownFacetTag::LabelGeneric).to_string();
    let working_facet_target = resolve_facet_write_target(
        &args.primary_doc.facets,
        &args.primary_doc.tags,
        &label_facet_key,
        &label_facet_tag,
        "labelGeneric",
    )?;

    use daybook_types::doc::WellKnownFacet;

    cx.effect(|| {
        let new_facet: daybook_types::doc::FacetRaw =
            WellKnownFacet::LabelGeneric("test_label".into()).into();
        let new_facet = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        working_facet_target.write(
            &new_facet,
            "access error updating facet",
            "access error creating facet",
        )?;
        Ok(Json(()))
    })?;

    Ok(())
}
