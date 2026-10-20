use daybook_types::doc::NoteEditorConfig;
use daybook_types::doc::NoteMimeOption;

fn hledger_note_mime_option() -> NoteMimeOption {
    NoteMimeOption {
        mime: crate::HLEDGER_NOTE_MIME.into(),
        label: "hledger journal".into(),
        description: "Ledger-style journal entry format.".into(),
    }
}

pub fn upsert_hledger_note_mime_option(mut config: NoteEditorConfig) -> NoteEditorConfig {
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

pub fn note_editor_config_facet_key() -> String {
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
