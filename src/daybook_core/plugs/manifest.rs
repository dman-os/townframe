use crate::interlude::*;

use garde::Validate;

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

pub fn is_domain_name(value: &str, _context: &()) -> garde::Result {
    if let Err(err) = addr::parse_domain_name(value) {
        return Err(garde::Error::new(format!(
            "error parsing prop tag \"{value}\": {err}"
        )));
    }
    Ok(())
}

#[derive(Debug, Validate, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(transparent)]
#[garde(transparent)]
#[repr(transparent)]
pub struct PropTag(#[garde(custom(is_domain_name))] String);

impl<T> From<T> for PropTag
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for PropTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for PropTag {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Validate, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
#[garde(transparent)]
#[repr(transparent)]
#[serde(rename_all = "camelCase")]
pub struct KeyGeneric(
    #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 1024))] pub String,
);

impl<T> From<T> for KeyGeneric
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for KeyGeneric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl garde::error::PathComponentKind for KeyGeneric {
    fn component_kind() -> garde::error::Kind {
        garde::error::Kind::Key
    }
}

impl std::ops::Deref for KeyGeneric {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Versions work lik @foo/bar@1.2.3
#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlugManifest {
    #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 32))]
    pub namespace: String,
    #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 32))]
    pub name: String,
    #[garde(skip)]
    pub version: semver::Version,

    #[garde(length(min = 1))]
    pub title: String,
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub props: Vec<PropKeyManifest>,
    // plugin_id: ->
    #[garde(dive)]
    pub dependencies: HashMap<String, Arc<PlugDependencyManifest>>,
    #[garde(dive)]
    pub routines: HashMap<KeyGeneric, Arc<RoutineManifest>>,
    #[garde(dive)]
    pub wflow_bundles: HashMap<KeyGeneric, Arc<WflowBundleManifest>>,
    #[garde(dive)]
    pub commands: Vec<Arc<CommandManifest>>,
    // processors: Vec<PluginProcessorManifest>,
}

impl PlugManifest {
    pub fn id(&self) -> String {
        format!("@{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PropKeyManifest {
    /// Must be reverse domain notation
    #[garde(dive)]
    pub key_tag: PropTag,
    #[garde(skip)]
    pub value_schema: schemars::Schema,
    #[garde(dive)]
    #[serde(default)]
    pub display_config: PropKeyDisplayHint,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlugDependencyManifest {
    #[garde(dive)]
    pub keys: Vec<PropKeyDependencyManifest>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PropKeyDependencyManifest {
    #[garde(dive)]
    pub key_tag: PropTag,
    #[garde(skip)]
    pub value_schema: schemars::Schema,
}

#[derive(Debug, Serialize, Deserialize, Default, Validate, Clone)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PropKeyDisplayHint {
    #[serde(default)]
    #[garde(skip)]
    pub always_visible: bool,
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub display_title: Option<String>,
    #[serde(default)]
    #[garde(skip)]
    pub deets: PropKeyDisplayDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Reconcile, Hydrate, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum PropKeyDisplayDeets {
    #[default]
    DebugPrint,
    DateTime {
        display_type: DateTimePropDisplayType,
    },
    UnixPath,
    Title {
        show_editor: bool,
    },
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, Reconcile, Hydrate, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DateTimePropDisplayType {
    #[default]
    TimeAndDate,
    Relative,
    TimeOnly,
    DateOnly,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WflowBundleManifest {
    #[garde(dive)]
    pub keys: Vec<KeyGeneric>,
    #[garde(skip)]
    pub component_paths: Vec<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RoutineManifest {
    #[garde(dive)]
    pub r#impl: RoutineImpl,
    #[garde(dive)]
    pub deets: RoutineManifestDeets,
    #[garde(dive)]
    pub prop_acl: Vec<RoutinePropAccess>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RoutineImpl {
    Wflow {
        #[garde(dive)]
        key: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RoutineManifestDeets {
    /// Routine that can be invoked on a document with rw access on whole doc
    DocInvoke {},
    /// Routine that is invoked when a pending prop is in a doc with ro access
    /// to doc but rw access on prop.
    DocProp {
        #[garde(dive)]
        working_prop_tag: PropTag,
    },
    // DocCollator {},
    // PredicateToDocProp {},
    // InvokeOnPredicate {},
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RoutinePropAccess {
    #[garde(dive)]
    pub tag: PropTag,
    #[serde(default)]
    #[garde(skip)]
    pub read: bool,
    #[serde(default)]
    #[garde(skip)]
    pub write: bool,
    // #[serde(default)]
    // pub list: bool,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommandManifest {
    #[garde(dive)]
    pub name: KeyGeneric,
    #[garde(length(min = 1))]
    pub desc: String,

    // cli_unlisted: bool,
    // gui_unlisted: bool,
    #[garde(dive)]
    pub deets: CommandDeets,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CommandDeets {
    // NOTE: behavior differs depending on the routine
    //  - if a DocInvoke, it's invoked
    //  - If a DocProp routine, the prop is added with pending payload
    //  - if InvokeOnPredicate, we re-check for predicates and run on matches
    //  - if PredicateToDocProp, we check predicates and add props
    //  - if DocCollator, we just run collator
    DocCommand {
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
}
