use crate::interlude::*;

use garde::Validate;

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

pub fn is_domain_name(value: &str, _context: &()) -> garde::Result {
    if let Err(err) = addr::parse_domain_name(value) {
        return Err(garde::Error::new(format!(
            "error parsing facet tag \"{value}\": {err}"
        )));
    }
    Ok(())
}

#[derive(Debug, Validate, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(transparent)]
#[garde(transparent)]
#[repr(transparent)]
pub struct FacetTag(#[garde(custom(is_domain_name))] pub String);

impl<T> From<T> for FacetTag
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for FacetTag {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{}", self.0)
    }
}

impl std::ops::Deref for FacetTag {
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

impl std::borrow::Borrow<str> for KeyGeneric {
    fn borrow(&self) -> &str {
        &self[..]
    }
}

impl<T> From<T> for KeyGeneric
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for KeyGeneric {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{}", self.0)
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
    pub facets: Vec<FacetManifest>,
    #[garde(dive)]
    #[serde(default)]
    pub local_states: HashMap<KeyGeneric, Arc<LocalStateManifest>>,
    // plugin_id: ->
    #[garde(dive)]
    pub dependencies: HashMap<String, Arc<PlugDependencyManifest>>,
    #[garde(dive)]
    pub routines: HashMap<KeyGeneric, Arc<RoutineManifest>>,
    #[garde(dive)]
    pub wflow_bundles: HashMap<KeyGeneric, Arc<WflowBundleManifest>>,
    #[garde(dive)]
    pub commands: HashMap<KeyGeneric, Arc<CommandManifest>>,
    #[garde(dive)]
    pub processors: HashMap<KeyGeneric, Arc<ProcessorManifest>>,
}

impl PlugManifest {
    pub fn id(&self) -> String {
        format!("@{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FacetManifest {
    /// Must be reverse domain notation
    #[garde(dive)]
    pub key_tag: FacetTag,
    #[garde(skip)]
    pub value_schema: schemars::Schema,
    #[garde(dive)]
    #[serde(default)]
    pub display_config: FacetDisplayHint,
    #[garde(dive)]
    #[serde(default)]
    pub references: Vec<FacetReferenceManifest>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct FacetReferenceManifest {
    #[garde(dive)]
    pub reference_kind: FacetReferenceKind,
    /// JSON pointer (e.g. `/facetRef`) or root-dot path (e.g. `$.facetRef`)
    #[garde(length(min = 1))]
    pub json_path: String,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum FacetReferenceKind {
    UrlFacet,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlugDependencyManifest {
    #[garde(dive)]
    #[serde(default)]
    pub keys: Vec<FacetDependencyManifest>,
    #[garde(dive)]
    #[serde(default)]
    pub local_states: Vec<LocalStateDependencyManifest>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FacetDependencyManifest {
    #[garde(dive)]
    pub key_tag: FacetTag,
    #[garde(skip)]
    pub value_schema: schemars::Schema,
}

#[derive(Debug, Serialize, Deserialize, Default, Validate, Clone)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct FacetDisplayHint {
    #[serde(default)]
    #[garde(skip)]
    pub always_visible: bool,
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub display_title: Option<String>,
    #[serde(default)]
    #[garde(skip)]
    pub deets: FacetKeyDisplayDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Reconcile, Hydrate, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum FacetKeyDisplayDeets {
    #[default]
    DebugPrint,
    DateTime {
        display_type: DateTimeFacetDisplayType,
    },
    UnixPath,
    Title {
        show_editor: bool,
    },
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, Reconcile, Hydrate, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DateTimeFacetDisplayType {
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
    pub component_urls: Vec<Url>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RoutineManifest {
    #[garde(dive)]
    pub r#impl: RoutineImpl,
    #[garde(dive)]
    pub deets: RoutineManifestDeets,
    #[garde(dive)]
    pub facet_acl: Vec<RoutineFacetAccess>,
    #[garde(dive)]
    #[serde(default)]
    pub local_state_acl: Vec<RoutineLocalStateAccess>,
}

impl RoutineManifest {
    /// Read set for short-circuit: tag-level (any id) and key-level (tag+id when key_id is set).
    /// Returns (read_tags, read_keys). Predicates/triage match by tag; when key_id is set, match by full key.
    pub fn read_facet_set(
        &self,
    ) -> (
        std::collections::HashSet<String>,
        std::collections::HashSet<daybook_types::doc::FacetKey>,
    ) {
        use daybook_types::doc::{FacetKey, FacetTag};
        let mut read_tags = std::collections::HashSet::new();
        let mut read_keys = std::collections::HashSet::new();
        for access in &self.facet_acl {
            if !access.read {
                continue;
            }
            let tag_str = access.tag.0.as_str();
            if let Some(ref id) = access.key_id {
                read_keys.insert(FacetKey {
                    tag: FacetTag::from(tag_str),
                    id: id.clone(),
                });
            } else {
                read_tags.insert(access.tag.0.clone());
            }
        }
        if let RoutineManifestDeets::DocFacet { working_facet_tag } = &self.deets {
            read_tags.insert(working_facet_tag.0.clone());
        }
        (read_tags, read_keys)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RoutineImpl {
    Wflow {
        #[garde(dive)]
        bundle: KeyGeneric,
        #[garde(dive)]
        key: KeyGeneric,
    },
}

// FIXME: this is poorly designed
#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RoutineManifestDeets {
    /// Routine that can be invoked on a document with rw access on whole doc
    /// FIXME: remove this branch?
    DocInvoke {},
    /// Routine that is invoked when with ro access
    /// to doc but rw access on facet.
    DocFacet {
        #[garde(dive)]
        working_facet_tag: FacetTag,
    },
    // DocCollator { predicate },
    // DocPropCollator { predicate },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RoutineFacetAccess {
    #[garde(dive)]
    pub tag: FacetTag,
    /// When set, access is to this tag+id only; when absent, access is to any facet with this tag.
    #[serde(default)]
    #[garde(skip)]
    pub key_id: Option<String>,
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
pub struct RoutineLocalStateAccess {
    #[garde(length(min = 1))]
    pub plug_id: String,
    #[garde(dive)]
    pub local_state_key: KeyGeneric,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommandManifest {
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
    DocCommand {
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorManifest {
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub deets: ProcessorDeets,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ProcessorDeets {
    /// Tests `predicate` whenever a doc changes and
    /// invokes routine if it's true.
    DocProcessor {
        #[garde(dive)]
        predicate: DocPredicateClause,
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
    // PropProcessor {}
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DocPredicateClause {
    HasTag(#[garde(dive)] FacetTag),
    Or(#[garde(dive)] Vec<Self>),
    And(#[garde(dive)] Vec<Self>),
    Not(#[garde(dive)] Box<Self>),
}

impl DocPredicateClause {
    pub fn matches(&self, doc: &daybook_types::doc::Doc) -> bool {
        match self {
            Self::HasTag(tag) => doc.facets.keys().any(|key| key.tag.to_string() == tag.0),
            Self::Or(clauses) => clauses.iter().any(|clause| clause.matches(doc)),
            Self::And(clauses) => clauses.iter().all(|clause| clause.matches(doc)),
            Self::Not(clause) => !clause.matches(doc),
        }
    }

    /// Collect all PropTags referenced by this predicate (for HasTag, the tag; for And/Or/Not, union from sub-clauses).
    pub fn referenced_tags(&self) -> std::collections::HashSet<FacetTag> {
        let mut out = std::collections::HashSet::new();
        self.collect_referenced_tags(&mut out);
        out
    }

    fn collect_referenced_tags(&self, out: &mut std::collections::HashSet<FacetTag>) {
        match self {
            Self::HasTag(tag) => {
                out.insert(tag.clone());
            }
            Self::Or(clauses) | Self::And(clauses) => {
                for clause in clauses {
                    clause.collect_referenced_tags(out);
                }
            }
            Self::Not(clause) => clause.collect_referenced_tags(out),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum LocalStateManifest {
    SqliteFile {},
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct LocalStateDependencyManifest {
    #[garde(dive)]
    pub local_state_key: KeyGeneric,
    #[garde(dive)]
    pub state_kind: LocalStateManifest,
}
