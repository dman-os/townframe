// FIXME: consistent cross plugin reference story
// FIXME: separate wasm components list

use crate::interlude::*;

use crate::doc::FacetRef;
use crate::reference::select_json_path_values;

use autosurgeon::Hydrate;
use autosurgeon::Reconcile;

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

#[derive(Debug, Validate, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(transparent)]
#[garde(transparent)]
#[repr(transparent)]
pub struct ManifestFacetTag(#[garde(custom(is_domain_name))] pub String);

pub type FacetTag = ManifestFacetTag;

impl<T> From<T> for ManifestFacetTag
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for ManifestFacetTag {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{}", self.0)
    }
}

impl std::ops::Deref for ManifestFacetTag {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(ManifestFacetTag, String);

#[derive(
    Debug,
    Validate,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Reconcile,
    Hydrate,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(KeyGeneric, String);

/// Versions work lik @foo/bar@1.2.3
#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PlugManifest {
    #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 32))]
    pub namespace: String,
    #[garde(ascii, pattern(USERNAME_REGEX), length(min = 3, max = 64))]
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
    #[serde(default)]
    pub views: HashMap<KeyGeneric, Arc<ViewManifest>>,
    #[garde(dive)]
    pub commands: HashMap<KeyGeneric, Arc<CommandManifest>>,
    #[garde(dive)]
    #[serde(default)]
    pub inits: HashMap<KeyGeneric, Arc<InitManifest>>,
    #[garde(dive)]
    pub processors: HashMap<KeyGeneric, Arc<ProcessorManifest>>,
}

impl PlugManifest {
    pub fn id(&self) -> String {
        format!("@{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "ty", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum FacetReferenceManifest {
    UrlString {
        /// JSON pointer (e.g. `/facetRef`) or root-dot path (e.g. `$.facetRef`)
        #[garde(length(min = 1))]
        json_path: String,
    },
    UrlStringSplit {
        /// JSON pointer (e.g. `/facetRef`) or root-dot path (e.g. `$.facetRef`)
        #[garde(length(min = 1))]
        json_path: String,
        /// Optional JSON path for commit-heads associated with this reference.
        #[garde(length(min = 1))]
        at_commit_json_path: String,
    },
    UrlStringMany {
        /// JSON pointer (e.g. `/order`) or root-dot path (e.g. `$.order`)
        #[garde(length(min = 1))]
        json_path: String,
    },
    UrlObject {
        /// JSON pointer (e.g. `/srcRef`) or root-dot path (e.g. `$.srcRef`)
        #[garde(length(min = 1))]
        json_path: String,
    },
    UrlObjectMany {
        /// JSON pointer (e.g. `/srcRefs`) or root-dot path (e.g. `$.srcRefs[*]`)
        #[garde(length(min = 1))]
        json_path: String,
    },
}

impl FacetReferenceManifest {
    pub fn reference_kind(&self) -> FacetReferenceKind {
        FacetReferenceKind::UrlFacet
    }

    pub fn json_path(&self) -> &str {
        match self {
            Self::UrlString { json_path }
            | Self::UrlStringSplit { json_path, .. }
            | Self::UrlStringMany { json_path }
            | Self::UrlObject { json_path }
            | Self::UrlObjectMany { json_path } => json_path,
        }
    }

    pub fn is_many(&self) -> bool {
        matches!(
            self,
            Self::UrlStringMany { .. } | Self::UrlObjectMany { .. }
        )
    }

    pub fn at_commit_json_path(&self) -> Option<&str> {
        match self {
            Self::UrlStringSplit {
                at_commit_json_path,
                ..
            } => Some(at_commit_json_path),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum FacetReferenceKind {
    UrlFacet,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FacetDependencyManifest {
    #[garde(dive)]
    pub key_tag: FacetTag,
    #[garde(skip)]
    pub value_schema: schemars::Schema,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ViewManifest {
    #[garde(length(min = 1))]
    pub title: String,
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub provider: ViewProviderManifest,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum ViewProviderManifest {
    StatelessWasm {
        #[garde(dive)]
        bundle: KeyGeneric,
        #[garde(dive)]
        export: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Default, Validate, Clone)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FacetDisplayHint {
    #[serde(default)]
    #[garde(skip)]
    pub always_visible: bool,
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub display_title: Option<String>,
    #[serde(default)]
    #[garde(skip)]
    pub deets: FacetDisplayDeets,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Reconcile, Hydrate)]
pub enum FacetDisplayDeets {
    #[default]
    DebugPrint,
    DateTime {
        display_type: DateTimeFacetDisplayType,
    },
    UnixPath,
    Title {
        show_editor: bool,
    },
    CustomView {
        view: ViewRef,
        mode: FacetViewMode,
        priority: i32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Reconcile, Hydrate)]
pub struct ViewRef {
    #[serde(default)]
    pub plug_id: Option<String>,
    pub view_key: KeyGeneric,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Reconcile, Hydrate)]
pub enum FacetViewMode {
    #[default]
    Display,
    Edit,
    DisplayAndEdit,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Reconcile, Hydrate)]
pub enum DateTimeFacetDisplayType {
    #[default]
    TimeAndDate,
    Relative,
    TimeOnly,
    DateOnly,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WflowBundleManifest {
    #[garde(dive)]
    pub keys: Vec<KeyGeneric>,
    #[garde(skip)]
    pub component_urls: Vec<Url>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RoutineDocAcl {
    #[garde(dive)]
    pub doc_predicate: DocPredicateClause,
    #[garde(dive)]
    #[serde(default)]
    pub facet_acl: Vec<RoutineFacetAccess>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RoutineManifest {
    #[garde(dive)]
    pub r#impl: RoutineImpl,
    #[garde(dive)]
    #[serde(default)]
    pub doc_acls: Vec<RoutineDocAcl>,
    #[garde(dive)]
    #[serde(default)]
    pub query_acls: Vec<DocPredicateClause>,
    #[garde(dive)]
    #[serde(default)]
    pub config_facet_acl: Vec<RoutineFacetAccess>,
    #[garde(skip)]
    #[serde(default)]
    pub command_invoke_acl: Vec<Url>,
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
        std::collections::HashSet<crate::doc::FacetKey>,
    ) {
        use crate::doc::FacetKey;
        use crate::doc::FacetTag;
        let mut read_tags = std::collections::HashSet::new();
        let mut read_keys = std::collections::HashSet::new();
        for doc_acl in &self.doc_acls {
            for access in &doc_acl.facet_acl {
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
        }
        (read_tags, read_keys)
    }

    /// Union of all facet ACLs across doc ACLs.
    pub fn facet_acl(&self) -> Vec<RoutineFacetAccess> {
        let mut out = Vec::new();
        for doc_acl in &self.doc_acls {
            out.extend(doc_acl.facet_acl.iter().cloned());
        }
        out
    }

    pub fn config_facet_acl(&self) -> &[RoutineFacetAccess] {
        self.config_facet_acl.as_slice()
    }

    pub fn command_invoke_acl(&self) -> &[Url] {
        self.command_invoke_acl.as_slice()
    }

    /// All facet tags referenced by this routine's ACLs and query ACLs.
    pub fn referenced_tags(&self) -> std::collections::HashSet<FacetTag> {
        let mut out = std::collections::HashSet::new();
        for doc_acl in &self.doc_acls {
            for tag in doc_acl.doc_predicate.referenced_tags() {
                out.insert(tag);
            }
            for access in &doc_acl.facet_acl {
                out.insert(access.tag.clone());
            }
        }
        for predicate in &self.query_acls {
            for tag in predicate.referenced_tags() {
                out.insert(tag);
            }
        }
        out
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum RoutineImpl {
    Wflow {
        #[garde(dive)]
        bundle: KeyGeneric,
        #[garde(dive)]
        key: KeyGeneric,
    },
}

#[derive(Debug, Default, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RoutineFacetAccess {
    /// Required for config_facet_acl entries to disambiguate owner config doc.
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub owner_plug_id: Option<String>,
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
    #[serde(default)]
    #[garde(skip)]
    pub create: bool,
    #[serde(default)]
    #[garde(skip)]
    pub delete: bool,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RoutineLocalStateAccess {
    #[garde(length(min = 1))]
    pub plug_id: String,
    #[garde(dive)]
    pub local_state_key: KeyGeneric,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum CommandDeets {
    DocCommand {
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InitManifest {
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub run_mode: InitRunMode,
    #[garde(dive)]
    pub deets: InitDeets,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[derive(Reconcile, Hydrate)]
pub enum InitRunMode {
    PerInstall,
    PerBoot,
    PerNode,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum InitDeets {
    InvokeRoutine {
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProcessorManifest {
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub deets: ProcessorDeets,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum ProcessorDeets {
    /// Tests `predicate` whenever a doc changes and
    /// invokes routine if it's true.
    DocProcessor {
        #[serde(default)]
        #[garde(dive)]
        event_predicate: ProcessorEventPredicate,
        #[garde(dive)]
        predicate: DocPredicateClause,
        #[serde(rename = "routineName")]
        #[garde(dive)]
        routine_name: KeyGeneric,
    },
    // PropProcessor {}
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ProcessorEventPredicate {
    #[serde(default)]
    #[garde(dive)]
    pub node_predicate: NodePredicate,
    #[serde(default)]
    #[garde(dive)]
    pub doc_change_predicate: DocChangePredicate,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum NodePredicate {
    ChangeOrigin(#[garde(dive)] ChangeOriginDeets),
}

impl Default for NodePredicate {
    fn default() -> Self {
        Self::ChangeOrigin(ChangeOriginDeets::Local)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum ChangeOriginDeets {
    #[default]
    Local,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum DocChangePredicate {
    #[default]
    Any,
    Added,
    Deleted,
    ChangedFacetTags(#[garde(dive)] Vec<FacetTag>),
    ChangedFacetKeys(#[garde(skip)] Vec<crate::doc::FacetKey>),
    AddedFacetTags(#[garde(dive)] Vec<FacetTag>),
    AddedFacetKeys(#[garde(skip)] Vec<crate::doc::FacetKey>),
    RemovedFacetTags(#[garde(dive)] Vec<FacetTag>),
    RemovedFacetKeys(#[garde(skip)] Vec<crate::doc::FacetKey>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocChangeKind {
    Added,
    Updated,
    Deleted,
}

impl DocChangePredicate {
    fn matches_tags(
        tags: &[FacetTag],
        keys: &std::collections::HashSet<crate::doc::FacetKey>,
    ) -> bool {
        keys.iter().any(|key| {
            tags.iter()
                .any(|tag| key.tag.to_string().as_str() == tag.0.as_str())
        })
    }

    pub fn evaluate_change(
        &self,
        kind: DocChangeKind,
        changed_facet_keys: Option<&std::collections::HashSet<crate::doc::FacetKey>>,
        added_facet_keys: Option<&std::collections::HashSet<crate::doc::FacetKey>>,
        removed_facet_keys: Option<&std::collections::HashSet<crate::doc::FacetKey>>,
    ) -> bool {
        match self {
            Self::Any => true,
            Self::Added => kind == DocChangeKind::Added,
            Self::Deleted => kind == DocChangeKind::Deleted,
            Self::ChangedFacetTags(tags) => {
                let Some(changed_facet_keys) = changed_facet_keys else {
                    return false;
                };
                Self::matches_tags(tags, changed_facet_keys)
            }
            Self::ChangedFacetKeys(keys) => {
                let Some(changed_facet_keys) = changed_facet_keys else {
                    return false;
                };
                keys.iter().any(|key| changed_facet_keys.contains(key))
            }
            Self::AddedFacetTags(tags) => {
                let Some(added_facet_keys) = added_facet_keys else {
                    return false;
                };
                Self::matches_tags(tags, added_facet_keys)
            }
            Self::AddedFacetKeys(keys) => {
                let Some(added_facet_keys) = added_facet_keys else {
                    return false;
                };
                keys.iter().any(|key| added_facet_keys.contains(key))
            }
            Self::RemovedFacetTags(tags) => {
                let Some(removed_facet_keys) = removed_facet_keys else {
                    return false;
                };
                Self::matches_tags(tags, removed_facet_keys)
            }
            Self::RemovedFacetKeys(keys) => {
                let Some(removed_facet_keys) = removed_facet_keys else {
                    return false;
                };
                keys.iter().any(|key| removed_facet_keys.contains(key))
            }
        }
    }

    pub fn append_referenced_facet_scope(
        &self,
        read_tags: &mut std::collections::HashSet<String>,
        read_keys: &mut std::collections::HashSet<crate::doc::FacetKey>,
    ) {
        match self {
            Self::Any | Self::Added | Self::Deleted => {}
            Self::ChangedFacetTags(tags) => {
                read_tags.extend(tags.iter().map(|tag| tag.0.clone()));
            }
            Self::ChangedFacetKeys(keys) => {
                read_keys.extend(keys.iter().cloned());
            }
            Self::AddedFacetTags(tags) => {
                read_tags.extend(tags.iter().map(|tag| tag.0.clone()));
            }
            Self::AddedFacetKeys(keys) => {
                read_keys.extend(keys.iter().cloned());
            }
            Self::RemovedFacetTags(tags) => {
                read_tags.extend(tags.iter().map(|tag| tag.0.clone()));
            }
            Self::RemovedFacetKeys(keys) => {
                read_keys.extend(keys.iter().cloned());
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DocPredicateClause {
    HasTag(#[garde(dive)] FacetTag),
    HasReferenceToTag {
        #[garde(dive)]
        source_tag: FacetTag,
        #[garde(dive)]
        target_tag: FacetTag,
    },
    FacetFieldMatch {
        #[garde(dive)]
        tag: FacetTag,
        #[garde(length(min = 1))]
        json_path: String,
        #[garde(skip)]
        operator: CompareOp,
        #[garde(skip)]
        value: serde_json::Value,
    },
    Or(#[garde(dive)] Vec<Self>),
    And(#[garde(dive)] Vec<Self>),
    Not(#[garde(dive)] Box<Self>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DocPredicateEvalRequirement {
    FullDoc,
    FacetsOfTag(FacetTag),
    FacetManifest,
}

#[derive(Debug, Clone)]
pub enum DocPredicateEvalResolved {
    FullDoc(Arc<crate::doc::Doc>),
    FacetsOfTag(Vec<(crate::doc::FacetKey, crate::doc::FacetRaw)>),
    FacetManifest(Arc<HashMap<String, Vec<FacetReferenceManifest>>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocPredicateEvalMode {
    ApproxInterest,
    Exact,
}

impl DocPredicateClause {
    pub fn append_requirements(
        &self,
        out: &mut std::collections::HashSet<DocPredicateEvalRequirement>,
    ) {
        match self {
            Self::HasTag(tag) => {
                out.insert(DocPredicateEvalRequirement::FacetsOfTag(tag.clone()));
            }
            Self::HasReferenceToTag { source_tag, .. } => {
                out.insert(DocPredicateEvalRequirement::FacetsOfTag(source_tag.clone()));
                out.insert(DocPredicateEvalRequirement::FacetManifest);
            }
            Self::FacetFieldMatch { tag, .. } => {
                out.insert(DocPredicateEvalRequirement::FacetsOfTag(tag.clone()));
            }
            Self::Or(clauses) | Self::And(clauses) => {
                for clause in clauses {
                    clause.append_requirements(out);
                }
            }
            Self::Not(clause) => clause.append_requirements(out),
        }
    }

    pub fn evaluate(
        &self,
        doc: &crate::doc::Doc,
        mode: DocPredicateEvalMode,
        resolved: &HashMap<DocPredicateEvalRequirement, DocPredicateEvalResolved>,
    ) -> bool {
        match self {
            Self::HasTag(tag) => doc.facets.keys().any(|key| key.tag.to_string() == tag.0),
            Self::HasReferenceToTag {
                source_tag,
                target_tag,
            } => {
                let Some(DocPredicateEvalResolved::FacetsOfTag(source_facets)) = resolved.get(
                    &DocPredicateEvalRequirement::FacetsOfTag(source_tag.clone()),
                ) else {
                    return match mode {
                        DocPredicateEvalMode::ApproxInterest => doc
                            .facets
                            .keys()
                            .any(|key| key.tag.to_string() == source_tag.0),
                        DocPredicateEvalMode::Exact => false,
                    };
                };

                let Some(DocPredicateEvalResolved::FacetManifest(facet_reference_specs)) =
                    resolved.get(&DocPredicateEvalRequirement::FacetManifest)
                else {
                    return match mode {
                        DocPredicateEvalMode::ApproxInterest => !source_facets.is_empty(),
                        DocPredicateEvalMode::Exact => false,
                    };
                };

                doc_facets_have_manifest_declared_reference_to_tag(
                    source_facets,
                    &source_tag.0,
                    &target_tag.0,
                    facet_reference_specs,
                )
            }
            Self::FacetFieldMatch {
                tag,
                json_path,
                operator,
                value,
            } => {
                let Some(DocPredicateEvalResolved::FacetsOfTag(source_facets)) =
                    resolved.get(&DocPredicateEvalRequirement::FacetsOfTag(tag.clone()))
                else {
                    return match mode {
                        DocPredicateEvalMode::ApproxInterest => {
                            doc.facets.keys().any(|key| key.tag.to_string() == tag.0)
                        }
                        DocPredicateEvalMode::Exact => false,
                    };
                };

                evaluate_facet_field_match(source_facets, json_path, *operator, value)
            }
            Self::Or(clauses) => clauses
                .iter()
                .any(|clause| clause.evaluate(doc, mode, resolved)),
            Self::And(clauses) => clauses
                .iter()
                .all(|clause| clause.evaluate(doc, mode, resolved)),
            Self::Not(clause) => !clause.evaluate(doc, mode, resolved),
        }
    }

    pub fn matches(&self, doc: &crate::doc::Doc) -> bool {
        self.evaluate(doc, DocPredicateEvalMode::ApproxInterest, &HashMap::new())
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
            Self::HasReferenceToTag {
                source_tag,
                target_tag,
            } => {
                out.insert(source_tag.clone());
                out.insert(target_tag.clone());
            }
            Self::FacetFieldMatch { tag, .. } => {
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

fn doc_facets_have_manifest_declared_reference_to_tag(
    source_facets: &[(crate::doc::FacetKey, crate::doc::FacetRaw)],
    source_tag: &str,
    target_tag: &str,
    facet_reference_specs: &HashMap<String, Vec<FacetReferenceManifest>>,
) -> bool {
    let Some(reference_specs) = facet_reference_specs.get(source_tag) else {
        return false;
    };

    for (facet_key, facet_raw) in source_facets {
        if facet_key.tag.to_string() != source_tag {
            continue;
        }
        if facet_has_reference_to_tag(facet_raw, target_tag, reference_specs) {
            return true;
        }
    }
    false
}

fn facet_has_reference_to_tag(
    facet_raw: &serde_json::Value,
    target_tag: &str,
    reference_specs: &[FacetReferenceManifest],
) -> bool {
    for reference_spec in reference_specs {
        let selected_values = match select_json_path_values(facet_raw, reference_spec.json_path()) {
            Ok(values) => values,
            Err(err) => {
                debug!(error = %err, json_path = %reference_spec.json_path(), "invalid facet reference json_path");
                continue;
            }
        };

        for selected in selected_values {
            match reference_spec {
                FacetReferenceManifest::UrlString { .. }
                | FacetReferenceManifest::UrlStringSplit { .. }
                | FacetReferenceManifest::UrlStringMany { .. } => {
                    let url_strings: Vec<&str> = match selected {
                        serde_json::Value::String(value) => vec![value.as_str()],
                        serde_json::Value::Array(items) => {
                            items.iter().filter_map(|item| item.as_str()).collect()
                        }
                        _ => Vec::new(),
                    };

                    for url_str in url_strings {
                        let Ok(matches_target) = crate::url::facet_ref_str_targets_tag(
                            url_str,
                            &crate::doc::FacetTag::from(target_tag),
                        ) else {
                            continue;
                        };
                        if matches_target {
                            return true;
                        }
                    }
                }
                FacetReferenceManifest::UrlObject { .. }
                | FacetReferenceManifest::UrlObjectMany { .. } => {
                    let Ok(facet_ref) = serde_json::from_value::<FacetRef>(selected.clone()) else {
                        continue;
                    };
                    let Ok(matches_target) = crate::url::facet_ref_targets_tag(
                        &facet_ref.r#ref,
                        &crate::doc::FacetTag::from(target_tag),
                    ) else {
                        continue;
                    };
                    if matches_target {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn evaluate_facet_field_match(
    facets: &[(crate::doc::FacetKey, crate::doc::FacetRaw)],
    json_path: &str,
    operator: CompareOp,
    expected: &serde_json::Value,
) -> bool {
    for (_, raw) in facets {
        let Ok(found) = select_json_path_values(raw, json_path) else {
            continue;
        };
        for found_val in found {
            if compare_json_values(found_val, operator, expected) {
                return true;
            }
        }
    }
    false
}

fn compare_json_values(lhs: &serde_json::Value, op: CompareOp, rhs: &serde_json::Value) -> bool {
    match op {
        CompareOp::Eq => compare_json_values_eq(lhs, rhs),
        CompareOp::Ne => !compare_json_values_eq(lhs, rhs),
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            if let Some(ordering) = compare_json_values_ordering(lhs, rhs) {
                return match op {
                    CompareOp::Gt => ordering.is_gt(),
                    CompareOp::Gte => ordering.is_ge(),
                    CompareOp::Lt => ordering.is_lt(),
                    CompareOp::Lte => ordering.is_le(),
                    _ => unreachable!(),
                };
            }
            if let (serde_json::Value::Number(lhs), serde_json::Value::Number(rhs)) = (lhs, rhs)
                && let (Some(lhs), Some(rhs)) = (lhs.as_f64(), rhs.as_f64())
            {
                return match op {
                    CompareOp::Gt => lhs > rhs,
                    CompareOp::Gte => lhs >= rhs,
                    CompareOp::Lt => lhs < rhs,
                    CompareOp::Lte => lhs <= rhs,
                    _ => unreachable!(),
                };
            }
            if let (Some(lhs), Some(rhs)) = (lhs.as_str(), rhs.as_str()) {
                return match op {
                    CompareOp::Gt => lhs > rhs,
                    CompareOp::Gte => lhs >= rhs,
                    CompareOp::Lt => lhs < rhs,
                    CompareOp::Lte => lhs <= rhs,
                    _ => unreachable!(),
                };
            }
            false
        }
    }
}

fn compare_json_values_eq(lhs: &serde_json::Value, rhs: &serde_json::Value) -> bool {
    match (lhs, rhs) {
        (serde_json::Value::Number(lhs), serde_json::Value::Number(rhs)) => {
            compare_json_numbers_eq(lhs, rhs)
        }
        _ => lhs == rhs,
    }
}

fn compare_json_numbers_eq(lhs: &serde_json::Number, rhs: &serde_json::Number) -> bool {
    if let Some(ordering) = compare_json_numbers_ordering(lhs, rhs) {
        return ordering.is_eq();
    }
    match (lhs.as_f64(), rhs.as_f64()) {
        (Some(lhs), Some(rhs)) => lhs == rhs,
        _ => false,
    }
}

fn compare_json_values_ordering(
    lhs: &serde_json::Value,
    rhs: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    match (lhs, rhs) {
        (serde_json::Value::Number(lhs), serde_json::Value::Number(rhs)) => {
            compare_json_numbers_ordering(lhs, rhs)
        }
        (serde_json::Value::String(lhs), serde_json::Value::String(rhs)) => Some(lhs.cmp(rhs)),
        _ => None,
    }
}

fn compare_json_numbers_ordering(
    lhs: &serde_json::Number,
    rhs: &serde_json::Number,
) -> Option<std::cmp::Ordering> {
    enum IntegerNumber {
        Signed(i64),
        Unsigned(u64),
    }

    fn integer_number(number: &serde_json::Number) -> Option<IntegerNumber> {
        number
            .as_i64()
            .map(IntegerNumber::Signed)
            .or_else(|| number.as_u64().map(IntegerNumber::Unsigned))
    }

    let lhs = integer_number(lhs)?;
    let rhs = integer_number(rhs)?;

    Some(match (lhs, rhs) {
        (IntegerNumber::Signed(lhs), IntegerNumber::Signed(rhs)) => lhs.cmp(&rhs),
        (IntegerNumber::Unsigned(lhs), IntegerNumber::Unsigned(rhs)) => lhs.cmp(&rhs),
        (IntegerNumber::Signed(lhs), IntegerNumber::Unsigned(rhs)) => {
            if lhs < 0 {
                std::cmp::Ordering::Less
            } else {
                (lhs as u64).cmp(&rhs)
            }
        }
        (IntegerNumber::Unsigned(lhs), IntegerNumber::Signed(rhs)) => {
            if rhs < 0 {
                std::cmp::Ordering::Greater
            } else {
                lhs.cmp(&(rhs as u64))
            }
        }
    })
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub enum LocalStateManifest {
    SqliteFile {},
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct LocalStateDependencyManifest {
    #[garde(dive)]
    pub local_state_key: KeyGeneric,
    #[garde(dive)]
    pub state_kind: LocalStateManifest,
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;
