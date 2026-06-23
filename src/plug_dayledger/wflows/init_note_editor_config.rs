use crate::interlude::*;
use daybook_types::doc::{NoteEditorConfig, NoteMimeOption};
use wflow_sdk::WflowCtx;

#[cfg_attr(
    test,
    expect(
        dead_code,
        reason = "entry point is used by the wasm route, not native unit tests"
    )
)]
pub fn run(_cx: &mut WflowCtx) -> Result<(), wflow_sdk::JobErrorX> {
    #[cfg(target_arch = "wasm32")]
    {
        run_wasm()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        Err(wflow_sdk::JobErrorX::Terminal(ferr!(
            "init_note_editor_config runs only as a wasm plugin routine"
        )))
    }
}

fn hledger_note_mime_option() -> NoteMimeOption {
    NoteMimeOption {
        mime: crate::HLEDGER_NOTE_MIME.into(),
        label: "hledger journal".into(),
        description: "Ledger-style journal entry format.".into(),
    }
}

fn upsert_hledger_note_mime_option(mut config: NoteEditorConfig) -> NoteEditorConfig {
    let hledger_option = hledger_note_mime_option();
    match config
        .mime_options
        .iter_mut()
        .find(|option| option.mime == hledger_option.mime)
    {
        Some(existing) => {
            *existing = hledger_option;
        }
        None => config.mime_options.push(hledger_option),
    }
    config
}

#[cfg(target_arch = "wasm32")]
fn run_wasm() -> Result<(), wflow_sdk::JobErrorX> {
    use crate::wit::townframe::daybook::capabilities::FacetRights;
    use crate::wit::townframe::daybook::facet_routine;

    let args = facet_routine::get_args();
    let core_config_doc = args
        .config_docs
        .iter()
        .find(|doc| {
            doc.tags.iter().any(|tag| {
                tag.tag() == crate::NOTE_EDITOR_CONFIG_FACET_TAG
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
        .find(|tag| tag.tag() == crate::NOTE_EDITOR_CONFIG_FACET_TAG)
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
        .create(crate::NOTE_EDITOR_CONFIG_FACET_ID, &next_raw)
        .map_err(|err| {
            wflow_sdk::JobErrorX::Terminal(ferr!(
                "error creating core note editor config facet: {err:?}"
            ))
        })?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn note_editor_config_facet_key() -> String {
    daybook_types::doc::FacetKey {
        tag: daybook_types::doc::FacetTag::from(crate::NOTE_EDITOR_CONFIG_FACET_TAG),
        id: crate::NOTE_EDITOR_CONFIG_FACET_ID.into(),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_hledger_note_mime_option_preserves_existing_options() {
        let config = NoteEditorConfig {
            mime_options: vec![NoteMimeOption {
                mime: "text/plain".into(),
                label: "Plain text".into(),
                description: "Existing option".into(),
            }],
        };

        let updated = upsert_hledger_note_mime_option(config);

        assert_eq!(
            updated,
            NoteEditorConfig {
                mime_options: vec![
                    NoteMimeOption {
                        mime: "text/plain".into(),
                        label: "Plain text".into(),
                        description: "Existing option".into(),
                    },
                    NoteMimeOption {
                        mime: crate::HLEDGER_NOTE_MIME.into(),
                        label: "hledger journal".into(),
                        description: "Ledger-style journal entry format.".into(),
                    },
                ],
            }
        );
    }

    #[test]
    fn upsert_hledger_note_mime_option_replaces_existing_hledger_option() {
        let config = NoteEditorConfig {
            mime_options: vec![
                NoteMimeOption {
                    mime: crate::HLEDGER_NOTE_MIME.into(),
                    label: "Old label".into(),
                    description: "Old description".into(),
                },
                NoteMimeOption {
                    mime: "text/plain".into(),
                    label: "Plain text".into(),
                    description: "Existing option".into(),
                },
            ],
        };

        let updated = upsert_hledger_note_mime_option(config);

        assert_eq!(
            updated.mime_options,
            vec![
                NoteMimeOption {
                    mime: crate::HLEDGER_NOTE_MIME.into(),
                    label: "hledger journal".into(),
                    description: "Ledger-style journal entry format.".into(),
                },
                NoteMimeOption {
                    mime: "text/plain".into(),
                    label: "Plain text".into(),
                    description: "Existing option".into(),
                },
            ],
        );
    }
}
