// Data exported from the pinned @pierre/highlights 0.4.0 dist by
// `oracle/generate.mjs` into `highlights/data/highlights_data.json`: the
// language name table (dist/languages.js), the token slot names
// (dist/token-types.js) and the Zed theme objects of the two Pierre themes
// (dist/themes/pierre-{dark,light}.js). A new upstream version is a new export
// (`node oracle/generate.mjs`), not an edit.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::Value;

struct Data {
    languages: HashMap<String, u8>,
    token_types: Vec<String>,
    themes: Vec<(String, Value)>,
}

fn data() -> &'static Data {
    static DATA: OnceLock<Data> = OnceLock::new();
    DATA.get_or_init(|| {
        let json: Value = serde_json::from_str(include_str!("data/highlights_data.json"))
            .expect("highlights_data.json is valid JSON");
        let languages = json["languages"]
            .as_array()
            .expect("languages")
            .iter()
            .map(|entry| {
                (
                    entry[0].as_str().expect("language name").to_owned(),
                    u8::try_from(entry[1].as_u64().expect("language id")).expect("id fits u8"),
                )
            })
            .collect();
        let token_types = json["tokenTypes"]
            .as_array()
            .expect("tokenTypes")
            .iter()
            .map(|name| name.as_str().expect("token type").to_owned())
            .collect();
        let themes = json["themes"]
            .as_object()
            .expect("themes")
            .iter()
            .map(|(name, theme)| (name.clone(), theme.clone()))
            .collect();
        Data {
            languages,
            token_types,
            themes,
        }
    })
}

/// The wasm language id of an already lower-cased name or alias.
pub fn language_id(lowercase_name: &str) -> Option<u8> {
    data().languages.get(lowercase_name).copied()
}

/// The token slot names, indexed by the wasm's highlight ids.
pub fn token_types() -> &'static [String] {
    &data().token_types
}

/// The bundled Zed theme objects by name.
pub fn bundled_themes() -> impl Iterator<Item = (&'static str, &'static Value)> {
    data()
        .themes
        .iter()
        .map(|(name, theme)| (name.as_str(), theme))
}
