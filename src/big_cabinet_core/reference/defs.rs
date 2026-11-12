//! The Lexicon of the reference types (ADR 035 §2.6): the defs a datum field refers to in order to
//! hold a reference.

/// The Lexicon of the reference types (ADR 035 §2.6). A datum field holds a reference by having
/// its Lexicon type refer to one of these defs, the way an AT Protocol record refers to
/// `com.atproto.repo.strongRef`: the declaration travels inside the published Lexicon, so a peer and
/// the reference index see it without any sidecar.
pub const DEFS_NSID: &str = "org.example.daybook.defs";
/// `#datumRef`: a [`DatumReference`](super::DatumReference), a whole datum, in its stored form (no query, no fragment).
pub const DATUM_REF: &str = "org.example.daybook.defs#datumRef";
/// `#datumLocation`: a [`Location`](super::Location), a position inside a datum, in its stored form (no query).
pub const DATUM_LOCATION: &str = "org.example.daybook.defs#datumLocation";
/// `#datumPin`: a [`DatumPin`](super::DatumPin), a location and the heads it is read at.
pub const DATUM_PIN: &str = "org.example.daybook.defs#datumPin";
/// `#keyRef`: a [`KeyReference`](super::KeyReference), a content-addressed datum named by its key
/// alone, with no document (ADR 035 §2.9).
pub const KEY_REF: &str = "org.example.daybook.defs#keyRef";
/// The authoring annotation a Rust type's schema carries to say its position *is* the named
/// Lexicon def: projection emits a `ref` to it, and the sidecar overlay leaves it to the def.
pub const LEXICON_REF: &str = "x-lexicon-ref";

/// The type libraries the node itself provides: Lexicon files with no `main` that any
/// cartridge's Lexicon may refer to without requesting a contract or vendoring a copy. Every
/// consumer that resolves a cartridge's references reads this one list: admission treats a
/// reference into one as resolved and refuses a vendored copy that differs, and dispatch
/// compiles an operation's input and output with them carried. Only [`DEFS_NSID`] is here, not
/// every Lexicon the node boots with.
pub fn node_type_libraries() -> &'static [serde_json::Value] {
    static LIBRARIES: std::sync::LazyLock<[serde_json::Value; 1]> =
        std::sync::LazyLock::new(|| [defs_lexicon()]);
    &*LIBRARIES
}

/// The published [`DEFS_NSID`] Lexicon file. Every registration carries it, so any datum Lexicon,
/// a cartridge's included, can refer to its defs.
pub fn defs_lexicon() -> serde_json::Value {
    serde_json::json!({
        "lexicon": 1,
        "id": DEFS_NSID,
        "defs": {
            "datumRef": {
                "type": "string",
                "format": "uri",
                "description": "A whole datum: a `dtxt:datum:` address with no query and no fragment.",
            },
            "datumLocation": {
                "type": "string",
                "format": "uri",
                "description": "A position inside a datum: a `dtxt:datum:` address with no query; a fragment, when present, is interpreted by the datum's type.",
            },
            "datumPin": {
                "type": "object",
                "required": ["ref"],
                "properties": {
                    "ref": { "type": "ref", "ref": "#datumLocation" },
                    "heads": {
                        "type": "array",
                        "minLength": 1,
                        "items": { "type": "string" },
                        "description": "The document heads the location is read at, as multibase base58btc. Absent: the document's current frontier.",
                    },
                },
            },
            "keyRef": {
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "A datum key, `<tag>/<slot>`, of a kind whose slot is a content hash of its value. Any document holding that key with a value that hashes to it satisfies the reference.",
                    },
                },
            },
        },
    })
}
