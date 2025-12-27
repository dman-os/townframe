use crate::interlude::*;

fn system_plugins() -> Vec<PluginManifest> {
    use daybook_types::doc::*;
    vec![
        //
        PluginManifest {
            namespace: "daybook".into(),
            name: "core".into(),
            version: "v0.0.1".parse().unwrap(),
            title: "Daybook Core".into(),
            desc: "Core keys and routines".into(),
            routines: default(),
            props: vec![
                PropKeyManifest {
                    key_tag: WellKnownPropTag::RefGeneric.to_string(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::LabelGeneric.to_string(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::PseudoLabel.to_string(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::TitleGeneric.to_string(),
                    value_schema: schemars::schema_for!(String),
                    display_config: PropKeyDisplayGuides {
                        display_type: MetaTableKeyDisplayType::Title { show_editor: true },
                        ..default()
                    },
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::PathGeneric.to_string(),
                    value_schema: schemars::schema_for!(String),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::ImageMetadata.to_string(),
                    value_schema: schemars::schema_for!(ImageMetadata),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::Content.to_string(),
                    value_schema: schemars::schema_for!(Content),
                    display_config: default(),
                },
                PropKeyManifest {
                    key_tag: WellKnownPropTag::Pending.to_string(),
                    value_schema: schemars::schema_for!(Pending),
                    display_config: default(),
                },
            ],
        },
    ]
}

/// Versions work lik @foo/bar@1.2.3
pub struct PluginManifest {
    pub namespace: String,
    pub name: String,
    pub version: semver::Version,

    pub title: String,
    pub desc: String,
    // dependencies: Vec<String>,
    pub routines: HashMap<String, RoutineManifest>,
    pub props: Vec<PropKeyManifest>,
    // commands: Vec<PluginCommandManifest>,
    // processors: Vec<PluginProcessorManifest>,
}

pub struct PropKeyManifest {
    /// Must be reverse domain notation
    pub key_tag: String,
    pub value_schema: schemars::Schema,
    pub display_config: PropKeyDisplayGuides,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PropKeyDisplayGuides {
    #[serde(default)]
    pub always_visible: bool,
    #[serde(default)]
    pub display_type: MetaTableKeyDisplayType,
    #[serde(default)]
    pub display_title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DateTimeDisplayType {
    #[default]
    TimeAndDate,
    Relative,
    TimeOnly,
    DateOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum MetaTableKeyDisplayType {
    #[default]
    DebugPrint,
    DateTime {
        display_type: DateTimeDisplayType,
    },
    UnixPath,
    Title {
        show_editor: bool,
    },
}

pub struct RoutineManifest {}

pub enum RoutineManifestDeets {
    /// Routine that can be invoked on a document with rw access on whole doc
    DocInvoke {},
    /// Routine that is invoked when a pending prop is in a doc with ro access
    /// to doc but rw access on prop.
    DocProp {},
    // DocCollator {},
    // PredicateToDocProp {},
    // InvokeOnPredicate {},
}

pub struct CommandManifest {
    name: String,
    desc: String,

    // cli_unlisted: bool,
    // gui_unlisted: bool,
    deets: CommandDeets,
}

pub enum CommandDeets {
    // NOTE: behavior differs depending on the routine
    //  - if a DocInvoke, it's invoked
    //  - If a DocProp routine, the prop is added with pending payload
    //  - if InvokeOnPredicate, we re-check for predicates and run on matches
    //  - if PredicateToDocProp, we check predicates and add props
    //  - if DocCollator, we just run collator
    DocCommand { routine_name: String },
}

// struct PluginProcessorManifest {}

//
//
// enum CommandImpl {
//     Wflow(),
// }
