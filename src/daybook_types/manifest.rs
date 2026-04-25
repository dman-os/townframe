use crate::interlude::*;

use crate::reference::select_json_path_values;

#[cfg(feature = "automerge")]
use autosurgeon::{Hydrate, Reconcile};

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
    /// Optional JSON path for commit-heads associated with this reference.
    ///
    /// Convention:
    /// - When present and the selected value is exactly `[]`, this means "self":
    ///   the referenced facet must be in the same validated facet set.
    /// - When absent, commit heads must be encoded in the reference URL fragment
    ///   as pipe-separated hashes (e.g. `#h1|h2|h3`).
    #[serde(default)]
    #[garde(inner(length(min = 1)))]
    pub at_commit_json_path: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
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

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
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
pub struct RoutineDocAcl {
    #[garde(dive)]
    pub doc_predicate: DocPredicateClause,
    #[garde(dive)]
    #[serde(default)]
    pub facet_acl: Vec<RoutineFacetAccess>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
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
        use crate::doc::{FacetKey, FacetTag};
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
#[serde(rename_all = "camelCase")]
pub enum RoutineImpl {
    Wflow {
        #[garde(dive)]
        bundle: KeyGeneric,
        #[garde(dive)]
        key: KeyGeneric,
    },
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
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
pub struct InitManifest {
    #[garde(length(min = 1))]
    pub desc: String,
    #[garde(dive)]
    pub run_mode: InitRunMode,
    #[garde(dive)]
    pub deets: InitDeets,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "automerge", derive(Reconcile, Hydrate))]
pub enum InitRunMode {
    PerInstall,
    PerBoot,
    PerNode,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
#[serde(rename_all = "camelCase")]
pub enum InitDeets {
    InvokeRoutine {
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
#[serde(rename_all = "camelCase")]
pub enum ChangeOriginDeets {
    #[default]
    Local,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone, Default)]
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
#[serde(rename_all = "camelCase")]
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
            Self::HasTag(_) => {}
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
        match reference_spec.reference_kind {
            FacetReferenceKind::UrlFacet => {}
        }

        let selected_values = match select_json_path_values(facet_raw, &reference_spec.json_path) {
            Ok(values) => values,
            Err(err) => {
                debug!(error = %err, json_path = %reference_spec.json_path, "invalid facet reference json_path");
                continue;
            }
        };

        for selected in selected_values {
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
    }
    false
}

fn evaluate_facet_field_match(
    facets: &[(crate::doc::FacetKey, crate::doc::FacetRaw)],
    json_path: &str,
    operator: CompareOp,
    expected: &serde_json::Value,
) -> bool {
    use jsonpath_rust::JsonPath;
    for (_, raw) in facets {
        let Ok(found) = raw.query(json_path) else {
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
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            if let (Some(lhs), Some(rhs)) = (lhs.as_f64(), rhs.as_f64()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn processor_event_predicate_defaults_to_local_any() {
        let predicate = ProcessorEventPredicate::default();
        assert!(matches!(
            predicate.node_predicate,
            NodePredicate::ChangeOrigin(ChangeOriginDeets::Local)
        ));
        assert!(matches!(
            predicate.doc_change_predicate,
            DocChangePredicate::Any
        ));
    }

    #[test]
    fn doc_processor_event_predicate_is_optional_in_serde() {
        let json = serde_json::json!({
            "desc": "x",
            "deets": {
                "docProcessor": {
                    "predicate": { "hasTag": "org.example.tag" },
                    "routineName": "routine1"
                }
            }
        });
        let manifest: ProcessorManifest = serde_json::from_value(json).expect("valid manifest");
        let ProcessorDeets::DocProcessor {
            event_predicate, ..
        } = manifest.deets;
        assert!(matches!(
            event_predicate.node_predicate,
            NodePredicate::ChangeOrigin(ChangeOriginDeets::Local)
        ));
        assert!(matches!(
            event_predicate.doc_change_predicate,
            DocChangePredicate::Any
        ));
    }

    #[test]
    fn doc_change_predicate_changed_facet_tags_evaluate_change() {
        use std::collections::HashSet;

        let changed: HashSet<crate::doc::FacetKey> = vec![
            crate::doc::FacetKey {
                tag: "org.example.note".into(),
                id: "main".into(),
            },
            crate::doc::FacetKey {
                tag: "org.example.todo".into(),
                id: "1".into(),
            },
        ]
        .into_iter()
        .collect();

        let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()]);
        assert!(pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));

        let pred = DocChangePredicate::ChangedFacetTags(vec!["org.example.unknown".into()]);
        assert!(!pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
    }

    #[test]
    fn doc_change_predicate_changed_facet_keys_evaluate_change() {
        use std::collections::HashSet;

        let target_key = crate::doc::FacetKey {
            tag: "org.example.todo".into(),
            id: "1".into(),
        };
        let changed: HashSet<crate::doc::FacetKey> = vec![target_key.clone()].into_iter().collect();

        let pred = DocChangePredicate::ChangedFacetKeys(vec![target_key]);
        assert!(pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
        assert!(!pred.evaluate_change(DocChangeKind::Updated, None, None, None));

        let pred = DocChangePredicate::ChangedFacetKeys(vec![crate::doc::FacetKey {
            tag: "org.example.todo".into(),
            id: "nope".into(),
        }]);
        assert!(!pred.evaluate_change(DocChangeKind::Updated, Some(&changed), None, None));
    }

    #[test]
    fn doc_change_predicate_append_referenced_facet_scope() {
        use std::collections::HashSet;

        let mut read_tags = HashSet::new();
        let mut read_keys = HashSet::new();
        DocChangePredicate::ChangedFacetTags(vec!["org.example.todo".into()])
            .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
        assert!(read_tags.contains("org.example.todo"));
        assert!(read_keys.is_empty());

        let key = crate::doc::FacetKey {
            tag: "org.example.todo".into(),
            id: "1".into(),
        };
        DocChangePredicate::ChangedFacetKeys(vec![key.clone()])
            .append_referenced_facet_scope(&mut read_tags, &mut read_keys);
        assert!(read_keys.contains(&key));
    }

    #[test]
    fn facet_field_match_eq_string() {
        let tag: FacetTag = "org.example.note".into();
        let note_json = serde_json::json!({
            "mime": "text/x-hledger-journal",
            "content": "2024-01-01 test\n  assets:cash  $10\n  expenses:food"
        });
        let facets = vec![(
            crate::doc::FacetKey {
                tag: "org.example.note".into(),
                id: "main".into(),
            },
            note_json,
        )];

        let predicate = DocPredicateClause::FacetFieldMatch {
            tag: tag.clone(),
            json_path: "$.mime".into(),
            operator: CompareOp::Eq,
            value: serde_json::Value::String("text/x-hledger-journal".into()),
        };

        let mut requirements = HashSet::new();
        predicate.append_requirements(&mut requirements);
        assert!(requirements.contains(&DocPredicateEvalRequirement::FacetsOfTag(tag.clone())));

        let mut resolved = HashMap::new();
        resolved.insert(
            DocPredicateEvalRequirement::FacetsOfTag(tag),
            DocPredicateEvalResolved::FacetsOfTag(facets),
        );

        let doc = crate::doc::Doc {
            id: "doc1".into(),
            facets: HashMap::new(),
        };

        assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
    }

    #[test]
    fn facet_field_match_ne_string() {
        let tag: FacetTag = "org.example.note".into();
        let note_json = serde_json::json!({
            "mime": "text/plain",
            "content": "hello"
        });
        let facets = vec![(
            crate::doc::FacetKey {
                tag: "org.example.note".into(),
                id: "main".into(),
            },
            note_json,
        )];

        let predicate = DocPredicateClause::FacetFieldMatch {
            tag: tag.clone(),
            json_path: "$.mime".into(),
            operator: CompareOp::Ne,
            value: serde_json::Value::String("text/x-hledger-journal".into()),
        };

        let mut resolved = HashMap::new();
        resolved.insert(
            DocPredicateEvalRequirement::FacetsOfTag(tag),
            DocPredicateEvalResolved::FacetsOfTag(facets),
        );

        let doc = crate::doc::Doc {
            id: "doc1".into(),
            facets: HashMap::new(),
        };

        assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
    }

    #[test]
    fn facet_field_match_gt_numeric() {
        let tag: FacetTag = "org.example.note".into();
        let note_json = serde_json::json!({
            "confidence": 0.95
        });
        let facets = vec![(
            crate::doc::FacetKey {
                tag: "org.example.note".into(),
                id: "main".into(),
            },
            note_json,
        )];

        let predicate = DocPredicateClause::FacetFieldMatch {
            tag: tag.clone(),
            json_path: "$.confidence".into(),
            operator: CompareOp::Gt,
            value: serde_json::json!(0.5),
        };

        let mut resolved = HashMap::new();
        resolved.insert(
            DocPredicateEvalRequirement::FacetsOfTag(tag),
            DocPredicateEvalResolved::FacetsOfTag(facets),
        );

        let doc = crate::doc::Doc {
            id: "doc1".into(),
            facets: HashMap::new(),
        };

        assert!(predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
    }

    #[test]
    fn facet_field_match_mismatch() {
        let tag: FacetTag = "org.example.note".into();
        let note_json = serde_json::json!({
            "mime": "text/plain"
        });
        let facets = vec![(
            crate::doc::FacetKey {
                tag: "org.example.note".into(),
                id: "main".into(),
            },
            note_json,
        )];

        let predicate = DocPredicateClause::FacetFieldMatch {
            tag: tag.clone(),
            json_path: "$.mime".into(),
            operator: CompareOp::Eq,
            value: serde_json::Value::String("text/x-hledger-journal".into()),
        };

        let mut resolved = HashMap::new();
        resolved.insert(
            DocPredicateEvalRequirement::FacetsOfTag(tag),
            DocPredicateEvalResolved::FacetsOfTag(facets),
        );

        let doc = crate::doc::Doc {
            id: "doc1".into(),
            facets: HashMap::new(),
        };

        assert!(!predicate.evaluate(&doc, DocPredicateEvalMode::Exact, &resolved));
    }

    #[test]
    fn facet_field_match_deserialization() {
        let json = serde_json::json!({
            "facetFieldMatch": {
                "tag": "org.example.note",
                "json_path": "$.mime",
                "operator": "eq",
                "value": "text/x-hledger-journal"
            }
        });

        let clause: DocPredicateClause = serde_json::from_value(json).expect("valid clause");
        assert!(matches!(clause, DocPredicateClause::FacetFieldMatch {
            json_path, ..
        } if json_path == "$.mime"));
    }

    #[test]
    fn compare_json_values_eq_neq() {
        assert!(compare_json_values(
            &serde_json::json!("hello"),
            CompareOp::Eq,
            &serde_json::json!("hello"),
        ));
        assert!(!compare_json_values(
            &serde_json::json!("hello"),
            CompareOp::Eq,
            &serde_json::json!("world"),
        ));
        assert!(compare_json_values(
            &serde_json::json!("hello"),
            CompareOp::Ne,
            &serde_json::json!("world"),
        ));
    }

    #[test]
    fn compare_json_values_numeric() {
        assert!(compare_json_values(
            &serde_json::json!(10.0),
            CompareOp::Gt,
            &serde_json::json!(5.0),
        ));
        assert!(!compare_json_values(
            &serde_json::json!(3.0),
            CompareOp::Gt,
            &serde_json::json!(5.0),
        ));
        assert!(compare_json_values(
            &serde_json::json!(5.0),
            CompareOp::Gte,
            &serde_json::json!(5.0),
        ));
        assert!(compare_json_values(
            &serde_json::json!(3.0),
            CompareOp::Lt,
            &serde_json::json!(5.0),
        ));
    }

    #[test]
    fn routine_manifest_read_facet_set_from_doc_acls() {
        let manifest = RoutineManifest {
            r#impl: RoutineImpl::Wflow {
                bundle: "test".into(),
                key: "test".into(),
            },
            doc_acls: vec![
                RoutineDocAcl {
                    doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                    facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.note".into(),
                            key_id: None,
                            read: true,
                            write: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.blob".into(),
                            key_id: Some("main".into()),
                            read: true,
                            write: true,
                        },
                    ],
                },
                RoutineDocAcl {
                    doc_predicate: DocPredicateClause::HasTag("org.example.todo".into()),
                    facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: "org.example.todo".into(),
                        key_id: None,
                        read: false,
                        write: true,
                    }],
                },
            ],
            query_acls: vec![],
            config_facet_acl: vec![],
            command_invoke_acl: vec![],
            local_state_acl: vec![],
        };

        let (read_tags, read_keys) = manifest.read_facet_set();
        assert!(read_tags.contains("org.example.note"));
        assert!(!read_tags.contains("org.example.blob"));
        assert!(!read_tags.contains("org.example.todo"));
        assert_eq!(read_keys.len(), 1);
        assert!(read_keys.contains(&crate::doc::FacetKey {
            tag: "org.example.blob".into(),
            id: "main".into(),
        }));
    }

    #[test]
    fn routine_manifest_facet_acl_unions_all_doc_acls() {
        let manifest = RoutineManifest {
            r#impl: RoutineImpl::Wflow {
                bundle: "test".into(),
                key: "test".into(),
            },
            doc_acls: vec![
                RoutineDocAcl {
                    doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                    facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: "org.example.note".into(),
                        key_id: None,
                        read: true,
                        write: false,
                    }],
                },
                RoutineDocAcl {
                    doc_predicate: DocPredicateClause::HasTag("org.example.todo".into()),
                    facet_acl: vec![RoutineFacetAccess {
                        owner_plug_id: None,
                        tag: "org.example.todo".into(),
                        key_id: None,
                        read: false,
                        write: true,
                    }],
                },
            ],
            query_acls: vec![],
            config_facet_acl: vec![],
            command_invoke_acl: vec![],
            local_state_acl: vec![],
        };

        let acl = manifest.facet_acl();
        assert_eq!(acl.len(), 2);
        assert!(acl.iter().any(|a| a.tag.0 == "org.example.note" && a.read));
        assert!(acl.iter().any(|a| a.tag.0 == "org.example.todo" && a.write));
    }

    #[test]
    fn routine_manifest_referenced_tags_includes_doc_predicate_and_query_acls() {
        let manifest = RoutineManifest {
            r#impl: RoutineImpl::Wflow {
                bundle: "test".into(),
                key: "test".into(),
            },
            doc_acls: vec![RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                facet_acl: vec![RoutineFacetAccess {
                    owner_plug_id: None,
                    tag: "org.example.blob".into(),
                    key_id: None,
                    read: true,
                    write: false,
                }],
            }],
            query_acls: vec![DocPredicateClause::HasTag("org.example.todo".into())],
            config_facet_acl: vec![],
            command_invoke_acl: vec![],
            local_state_acl: vec![],
        };

        let tags = manifest.referenced_tags();
        assert!(tags.contains(&FacetTag::from("org.example.note")));
        assert!(tags.contains(&FacetTag::from("org.example.blob")));
        assert!(tags.contains(&FacetTag::from("org.example.todo")));
    }

    #[test]
    fn routine_manifest_serializes_with_flat_acl_fields() {
        let manifest = RoutineManifest {
            r#impl: RoutineImpl::Wflow {
                bundle: "test".into(),
                key: "test".into(),
            },
            doc_acls: vec![RoutineDocAcl {
                doc_predicate: DocPredicateClause::HasTag("org.example.note".into()),
                facet_acl: vec![RoutineFacetAccess {
                    owner_plug_id: None,
                    tag: "org.example.note".into(),
                    key_id: None,
                    read: true,
                    write: false,
                }],
            }],
            query_acls: vec![DocPredicateClause::HasTag("org.example.todo".into())],
            config_facet_acl: vec![RoutineFacetAccess {
                owner_plug_id: Some("@daybook/test".into()),
                tag: "org.example.config".into(),
                key_id: None,
                read: true,
                write: true,
            }],
            command_invoke_acl: vec![],
            local_state_acl: vec![],
        };

        let json = serde_json::to_value(&manifest).expect("serialize");
        assert!(json.get("docAcls").is_some(), "docAcls field should exist");
        assert!(
            json.get("queryAcls").is_some(),
            "queryAcls field should exist"
        );
        assert!(
            json.get("configFacetAcl").is_some(),
            "configFacetAcl field should exist"
        );
        assert!(
            json.get("deets").is_none(),
            "old RoutineManifestDeets enum should not exist"
        );
    }

    #[test]
    fn routine_manifest_deserializes_flat_acl_fields() {
        let json = serde_json::json!({
            "impl": {
                "wflow": {
                    "bundle": "test",
                    "key": "test"
                }
            },
            "docAcls": [
                {
                    "docPredicate": { "hasTag": "org.example.note" },
                    "facetAcl": [
                        {
                            "tag": "org.example.note",
                            "read": true,
                            "write": false
                        }
                    ]
                }
            ],
            "queryAcls": [
                { "hasTag": "org.example.todo" }
            ],
            "configFacetAcl": [
                {
                    "tag": "org.example.config",
                    "read": true,
                    "write": true
                }
            ]
        });

        let manifest: RoutineManifest = serde_json::from_value(json).expect("deserialize");
        assert_eq!(manifest.doc_acls.len(), 1);
        assert_eq!(manifest.query_acls.len(), 1);
        assert_eq!(manifest.config_facet_acl.len(), 1);
        assert_eq!(manifest.local_state_acl.len(), 0);
    }
}
