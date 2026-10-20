use crate::interlude::*;
use daybook_types::doc::NoteEditorConfig;
use plug_dayledger_manifest::note_editor_config::note_editor_config_facet_key;
use plug_dayledger_manifest::note_editor_config::upsert_hledger_note_mime_option;
use wflow_sdk::WflowCtx;

pub fn run(_cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;

    let args = facet_routine::get_args();
    let core_config_doc = args
        .config_docs
        .iter()
        .find(|doc| {
            doc.tags.iter().any(|tag| {
                tag.tag() == plug_dayledger_manifest::NOTE_EDITOR_CONFIG_FACET_TAG
                    && tag.rights().contains(FacetRights::CREATE)
            }) || doc.facets.iter().any(|facet| {
                facet.key() == note_editor_config_facet_key()
                    && facet.rights().contains(FacetRights::UPDATE)
            })
        })
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("core note editor config doc token not found"))
        })?;

    let config_key = note_editor_config_facet_key();
    if let Some(config_token) = core_config_doc
        .facets
        .iter()
        .find(|facet| facet.key() == config_key)
    {
        if !config_token.rights().contains(FacetRights::READ)
            || !config_token.rights().contains(FacetRights::UPDATE)
        {
            return Err(wflow_sdk::JobErrorX::Terminal(ferr!(
                "core note editor config facet token missing READ or UPDATE right"
            )));
        }
        let raw = config_token.get().map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "error reading core note editor config facet: {err:?}"
            ))
        })?;
        let config = serde_json::from_str::<NoteEditorConfig>(&raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "error parsing core note editor config facet: {err}"
            ))
        })?;
        let next_raw =
            serde_json::to_string(&upsert_hledger_note_mime_option(config)).expect(ERROR_JSON);
        let update_result = config_token.update(&next_raw).map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "denied updating core note editor config facet: {err:?}"
            ))
        })?;
        update_result.map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "error updating core note editor config facet: {err:?}"
            ))
        })?;
        return Ok(());
    }

    let config_tag_token = core_config_doc
        .tags
        .iter()
        .find(|tag| tag.tag() == plug_dayledger_manifest::NOTE_EDITOR_CONFIG_FACET_TAG)
        .ok_or_else(|| {
            wflow_sdk::JobErrorX::Terminal(ferr!("core note editor config tag token not found"))
        })?;
    if !config_tag_token.rights().contains(FacetRights::CREATE) {
        return Err(wflow_sdk::JobErrorX::Terminal(ferr!(
            "core note editor config tag token missing CREATE right"
        )));
    }

    let next_raw = serde_json::to_string(&upsert_hledger_note_mime_option(NoteEditorConfig {
        mime_options: vec![],
    }))
    .expect(ERROR_JSON);
    config_tag_token
        .create(
            plug_dayledger_manifest::NOTE_EDITOR_CONFIG_FACET_ID,
            &next_raw,
        )
        .map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "error creating core note editor config facet: {err:?}"
            ))
        })?;
    Ok(())
}
