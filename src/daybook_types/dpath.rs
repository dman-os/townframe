//! Dpaths — daybook's path-shaped object labels (FDR 001).
//!
//! A dpath is a UTF-8 label with path syntax (`/inbox/hello.md`). It is *not* a
//! filesystem position and carries no uniqueness constraint: any number of
//! documents may claim the same dpath, concurrently and from different devices.
//! The real filesystem mapping is a *derived* function of the claimant set.
//!
//! This module is the pure addressing core:
//!
//! - [`Dpath`]: the label itself, parse/validate per FDR 001 §1 (byte-exact
//!   UTF-8, no case folding, no normalization, **no reserved namespaces**).
//! - [`DpathFacet`]: the facet value that declares a claim (FDR 001 §2).
//! - [`derive_tree`]: the deterministic dpath → real-path mapping implementing
//!   the collision rules 1–3 (FDR 001 §3), the materialized surfaces of §5
//!   (`/by-id`, checkout metadata dirs), and conflicts-as-opinions (§4).
//!
//! Nothing here touches automerge, the checkout store, or the real filesystem:
//! the input is a list of claims, the output is a tree of *derived* entries
//! with provenance. Two things are deliberately out of scope:
//!
//! - **binding stickiness** (FDR 001 §3 "binding stability"): "the file at this
//!   path today is at this path tomorrow" is a per-checkout property held by
//!   the transactional layer (ADR 011). This module is the *fresh* derivation.
//! - **lens entry-sets** (FDR 001 §9, ADR 012): one dpath maps to an entry
//!   *set*; only collisions *between* claimants are resolved here.

use crate::interlude::*;

use crate::doc::{DocId, FacetKey, FacetTag};
use crate::url::parse_facet_ref;

use serde::de::Error as DeError;

/// The facet tag under which dpath claims live (FDR 001 §2).
///
/// The facet **key-id is the dpath string itself**, leading `/` included, so a
/// claim on `/inbox/hello.md` is the facet key
/// `org.example.daybook.dpath//inbox/hello.md` — see [`Dpath::facet_key`].
pub const DPATH_FACET_TAG: &str = "org.example.daybook.dpath";

/// The reserved materialization surface holding dpathless documents (FDR 001 §5).
pub const BY_ID_SURFACE_NAME: &str = "by-id";

//----------------------------------------------------------------------------
// Dpath
//----------------------------------------------------------------------------

/// A dpath: a label with path syntax (FDR 001 §1).
///
/// Canonical form: exactly one leading `/`, no trailing `/`, no empty
/// segments, no `.`/`..` segments. Comparison is byte-exact (`Ord` is on the
/// raw bytes); labels are otherwise *unrestricted* — there are no reserved
/// names in daybook, and `/by-id/…` is a perfectly legal dpath. The reserved
/// surfaces are handled by collision resolution at materialization time
/// (FDR 001 §5), not by rewriting labels here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Dpath(String);

/// The typed read validates: a `Dpath` value is always a well-formed label.
/// Unvalidated input arrives as a raw facet key-id instead — see
/// [`Dpath::parse_facet_key`] and [`RawClaim`], which report malformed labels
/// rather than failing (FDR 001 §4).
impl<'de> Deserialize<'de> for Dpath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(DeError::custom)
    }
}

impl Dpath {
    /// Parse and validate a dpath label.
    pub fn parse(input: &str) -> Result<Self, DpathError> {
        let Some(rest) = input.strip_prefix('/') else {
            return Err(DpathError::MissingLeadingSlash {
                raw: input.to_string(),
            });
        };
        if rest.is_empty() {
            return Err(DpathError::Empty {
                raw: input.to_string(),
            });
        }
        if rest.ends_with('/') {
            return Err(DpathError::TrailingSlash {
                raw: input.to_string(),
            });
        }
        for (index, segment) in rest.split('/').enumerate() {
            if segment.is_empty() {
                return Err(DpathError::EmptySegment {
                    raw: input.to_string(),
                    index,
                });
            }
            if segment == "." || segment == ".." {
                return Err(DpathError::DotSegment {
                    raw: input.to_string(),
                    segment: segment.to_string(),
                    index,
                });
            }
        }
        Ok(Self(input.to_string()))
    }

    /// The canonical dpath string, leading `/` included.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The path segments, outermost first.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0[1..].split('/')
    }

    /// The number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments().count()
    }

    /// The last segment (the claimed name at its parent).
    pub fn file_name(&self) -> &str {
        self.0[self.0.rfind('/').unwrap_or(0) + 1..].trim_start_matches('/')
    }

    /// The extension of the last segment, if any (FDR 001 §9: a lens hint).
    pub fn extension(&self) -> Option<&str> {
        extension_of(self.file_name())
    }

    /// The parent dpath, or `None` at the top level.
    pub fn parent(&self) -> Option<Self> {
        let parent = &self.0[..self.0.rfind('/')?];
        if parent.is_empty() {
            return None;
        }
        Self::parse(parent).ok()
    }

    /// The facet key of a claim at this dpath (FDR 001 §2).
    pub fn facet_key(&self) -> FacetKey {
        FacetKey {
            tag: FacetTag::Any(DPATH_FACET_TAG.to_string()),
            id: self.0.clone(),
        }
    }

    /// Recover a dpath from a facet key, if the key is a dpath claim.
    ///
    /// `None` means the key is not a dpath facet; `Some(Err(..))` means it is a
    /// dpath facet whose key-id is malformed (FDR 001 §4: opinions are kept, the
    /// malformed ones are reported, never silently dropped).
    pub fn parse_facet_key(key: &FacetKey) -> Option<Result<Self, DpathError>> {
        if key.tag.to_string() != DPATH_FACET_TAG {
            return None;
        }
        Some(Self::parse(&key.id))
    }
}

impl std::str::FromStr for Dpath {
    type Err = DpathError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl std::fmt::Display for Dpath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a dpath label is not a valid dpath (FDR 001 §1).
#[derive(Debug, Clone, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum DpathError {
    /// a dpath must start with '/', got {raw}
    MissingLeadingSlash { raw: String },
    /// a dpath must have at least one segment, got {raw}
    Empty { raw: String },
    /// a dpath must not end with '/', got {raw}
    TrailingSlash { raw: String },
    /// a dpath must not contain an empty segment (segment {index}), got {raw}
    EmptySegment { raw: String, index: usize },
    /// a dpath segment must not be '.' or '..' (segment {index} is {segment}), got {raw}
    DotSegment {
        raw: String,
        segment: String,
        index: usize,
    },
}

/// The extension of a name, if it has one (a leading dot is not an extension).
fn extension_of(name: &str) -> Option<&str> {
    let (stem, extension) = name.rsplit_once('.')?;
    if stem.is_empty() || extension.is_empty() {
        return None;
    }
    Some(extension)
}

/// How many trailing spill suffixes a real name carries (FDR 001 §3 Rule 3).
fn d_suffix_count(name: &str, spill_suffix: &str) -> usize {
    let token = spill_suffix.trim_start_matches('.');
    if token.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut rest = name;
    while let Some((head, tail)) = rest.rsplit_once('.') {
        if tail != token {
            break;
        }
        count += 1;
        rest = head;
    }
    count
}

//----------------------------------------------------------------------------
// The dpath facet value
//----------------------------------------------------------------------------

/// The value of a dpath facet (FDR 001 §2).
///
/// An empty target list is the **default whole-document scope**: the lens set
/// materializes every user-visible facet of the doc at that path. A non-empty
/// list is a **selective claim** naming the facets that materialize.
///
/// The facet stays a pure address: no lens hints, no mime expectations, no lens
/// parameters (those live in separate facets resolved by the lens layer).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DpathFacet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<DpathTarget>,
}

/// One entry of a selective dpath claim (FDR 001 §2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DpathTarget {
    /// A `db+facet` URL (`db+facet:///<doc-id>/<tag>/<id>?branch=…&at=…`).
    pub facet_ref: Url,
    /// Head pins; absent and empty both mean "same transaction" (dict.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_heads: Option<Vec<String>>,
}

impl DpathFacet {
    /// A whole-document claim (default scope).
    pub const fn whole_document() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    /// Whether the claim covers the whole document (default scope).
    pub fn is_whole_document(&self) -> bool {
        self.targets.is_empty()
    }

    /// Check that every target reference is a parseable `db+facet` URL.
    ///
    /// A target that does not resolve is a *state* (`target-not-found`, FDR 001
    /// §6), not a rejected claim; this only reports the ones that are not even
    /// well-formed references.
    pub fn validate_targets(&self) -> Vec<DpathTargetIssue> {
        self.targets
            .iter()
            .enumerate()
            .filter_map(|(index, target)| {
                parse_facet_ref(&target.facet_ref)
                    .err()
                    .map(|error| DpathTargetIssue {
                        index,
                        facet_ref: target.facet_ref.to_string(),
                        reason: format!("{error:#}"),
                    })
            })
            .collect()
    }

    /// Parse the value shapes documented in FDR 001 §2:
    ///
    /// - `{}` or `null` — whole-document claim,
    /// - `{"targets": [ … ]}` — selective claim,
    /// - `{"facetRef": …, "refHeads": …}` — the single-target shorthand used by
    ///   the cross-doc example.
    ///
    /// Unknown keys beside a known one are ignored (forward compatibility: the
    /// facet value may gain keys later, and lens customization deliberately lives
    /// elsewhere). An object with neither `targets` nor `facetRef` is an error:
    /// the shape is one we write ourselves, so a wrong shape is a writer bug
    /// rather than an unvalidated external input (FDR 001 §4 keeps *merges*
    /// tolerant; this is the typed read).
    pub fn from_json_value(value: &serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::whole_document()),
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    return Ok(Self::whole_document());
                }
                if let Some(targets) = map.get("targets") {
                    let targets = serde_json::from_value::<Vec<DpathTarget>>(targets.clone())
                        .map_err(|error| format!("invalid dpath facet targets: {error}"))?;
                    return Ok(Self { targets });
                }
                if map.contains_key("facetRef") {
                    let target = serde_json::from_value::<DpathTarget>(value.clone())
                        .map_err(|error| format!("invalid dpath facet target: {error}"))?;
                    return Ok(Self {
                        targets: vec![target],
                    });
                }
                Err(format!(
                    "dpath facet value has neither 'targets' nor 'facetRef': {value}"
                ))
            }
            other => Err(format!("dpath facet value must be an object, got {other}")),
        }
    }
}

impl<'de> Deserialize<'de> for DpathFacet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::from_json_value(&value).map_err(DeError::custom)
    }
}

/// A target reference that is not a well-formed facet reference (FDR 001 §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathTargetIssue {
    pub index: usize,
    pub facet_ref: String,
    pub reason: String,
}

//----------------------------------------------------------------------------
// Claims
//----------------------------------------------------------------------------

/// A dpath claim exactly as it arrives from the CRDT: the facet key-id is a raw
/// string, because a replica may hold a malformed one (FDR 001 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawClaim {
    pub doc_id: DocId,
    /// The dpath as it appears in the facet key-id, unvalidated.
    pub dpath: String,
    pub facet: DpathFacet,
    /// The dpath facet's change metadata (`dmeta.createdAt`), FDR 001 §3
    /// tiebreaker 2. Unknown timestamps sort last.
    pub assigned_at: Option<Timestamp>,
}

impl RawClaim {
    /// Validate the raw dpath into a typed claim.
    pub fn validate(&self) -> Result<DpathClaim, DpathError> {
        Ok(DpathClaim {
            doc_id: self.doc_id.clone(),
            dpath: Dpath::parse(&self.dpath)?,
            facet: self.facet.clone(),
            assigned_at: self.assigned_at,
        })
    }
}

/// A validated dpath claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathClaim {
    pub doc_id: DocId,
    pub dpath: Dpath,
    pub facet: DpathFacet,
    pub assigned_at: Option<Timestamp>,
}

//----------------------------------------------------------------------------
// Reserved materialization surfaces
//----------------------------------------------------------------------------

/// A surface the materializer owns, which is not a dpath claim (FDR 001 §5).
///
/// Reserved surfaces win their literal real name; dpath claimants of the same
/// name spill into the `.d` suffix rules (so a claim on `/by-id/x` materializes
/// at `/by-id.d/x`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedSurface {
    pub name: String,
    pub kind: ReservedKind,
}

/// What a reserved surface holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservedKind {
    /// `/by-id`: dpathless documents, materialized as full JSON reprs.
    /// Documents that have dpaths do **not** appear here (FDR 001 §5).
    ById { doc_ids: Vec<DocId> },
    /// A checkout metadata directory (`.dtree`, `.dnode`): contents are not
    /// dpath claims, so the derived tree only holds the directory itself.
    MetadataDir,
}

impl ReservedSurface {
    /// The `/by-id` surface over the given dpathless documents.
    pub fn by_id(doc_ids: impl IntoIterator<Item = DocId>) -> Self {
        Self {
            name: BY_ID_SURFACE_NAME.to_string(),
            kind: ReservedKind::ById {
                doc_ids: doc_ids.into_iter().collect(),
            },
        }
    }

    /// A checkout metadata directory (FDR 002 owns the actual names).
    pub fn metadata_dir(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ReservedKind::MetadataDir,
        }
    }
}

//----------------------------------------------------------------------------
// Naming policy (FDR 001 Q1 and Rule 2/3 mechanics)
//----------------------------------------------------------------------------

/// The naming knobs the collision rules lean on.
///
/// Two are explicitly open in FDR 001 and are kept here in one place so revisiting
/// them is cheap:
///
/// - **Q1, the derived name scheme** inside collision directories. The lean in the
///   FDR is "preserve the dpath's extension, stay unbounded about the exact scheme";
///   v1 uses `<stem>~<identity-digest><.ext>`, a deterministic, legible, collision-free
///   name derived from the claimant's object identity.
/// - **Rule 2/3, the spill suffix**, `.d`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpathNamePolicy {
    /// Separator between the original stem and the identity digest (Q1).
    pub derived_separator: char,
    /// How many hex characters of the identity digest to keep (Q1).
    pub derived_digest_chars: usize,
    /// The suffix appended when a real name is contested (Rule 2 / Rule 3).
    pub spill_suffix: &'static str,
}

impl Default for DpathNamePolicy {
    fn default() -> Self {
        Self {
            derived_separator: '~',
            derived_digest_chars: 8,
            spill_suffix: ".d",
        }
    }
}

impl DpathNamePolicy {
    /// The contested-name fallback: append the spill suffix, keeping any
    /// extension at the end (`note.md` → `note.d.md`, `a.d` → `a.d.d`).
    pub fn spill_name(&self, name: &str) -> String {
        let suffix = self.spill_suffix.trim_start_matches('.');
        match extension_of(name) {
            Some(extension) => format!(
                "{}.{suffix}.{extension}",
                &name[..name.len() - extension.len() - 1]
            ),
            None => format!("{name}{}", self.spill_suffix),
        }
    }

    /// The unique name a claimant gets inside a collision directory (Q1).
    pub fn derived_name(&self, segment: &str, identity: &str, extension: Option<&str>) -> String {
        let stem = match extension_of(segment) {
            Some(extension) => &segment[..segment.len() - extension.len() - 1],
            None => segment,
        };
        let digest = identity_digest(identity);
        let keep = self.derived_digest_chars.min(digest.len());
        let digest = &digest[..keep];
        match extension {
            Some(extension) => format!(
                "{stem}{sep}{digest}.{extension}",
                sep = self.derived_separator
            ),
            None => format!("{stem}{sep}{digest}", sep = self.derived_separator),
        }
    }
}

/// The identity of a claimant: its document, plus the exact dpath it claims
/// (which *is* the dpath facet key-id, FDR 001 §2).
fn claimant_identity(doc_id: &str, dpath: &Dpath) -> String {
    format!("{doc_id}\u{0}{dpath}")
}

fn identity_digest(identity: &str) -> String {
    blake3::hash(identity.as_bytes()).to_hex().to_string()
}

//----------------------------------------------------------------------------
// The derived tree
//----------------------------------------------------------------------------

/// The derived real-filesystem mapping of a claimant set (FDR 001 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTree {
    root: Vec<DerivedEntry>,
    /// Every accepted claim, in tiebreaker order (FDR 001 §3): the opinions this
    /// tree was derived from, retained in full.
    claims: Vec<DerivedClaim>,
    rejections: Vec<DpathRejection>,
}

impl DerivedTree {
    /// The entries at the root of the derived tree.
    pub fn root(&self) -> &[DerivedEntry] {
        &self.root
    }

    /// Every accepted claim, in tiebreaker order (assignment timestamp, then
    /// lexicographic doc id).
    pub fn claims(&self) -> &[DerivedClaim] {
        &self.claims
    }

    /// Claims that contributed nothing, with the reason (FDR 001 §4: opinions
    /// are never silently dropped).
    pub fn rejections(&self) -> &[DpathRejection] {
        &self.rejections
    }

    /// Every real path in the tree, directories marked with a trailing `/`.
    pub fn paths(&self) -> Vec<String> {
        let mut out = Vec::new();
        for entry in &self.root {
            entry.collect_paths("", &mut out);
        }
        out
    }

    /// Look up the entry at a real path (as rendered by [`Self::paths`]).
    pub fn lookup(&self, path: &str) -> Option<&DerivedEntry> {
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty());
        let mut entries = self.root.as_slice();
        let mut found = None;
        for segment in segments {
            let entry = entries.iter().find(|entry| entry.name() == segment)?;
            found = Some(entry);
            entries = match entry {
                DerivedEntry::Dir { children, .. } => children.as_slice(),
                DerivedEntry::Claim { .. } | DerivedEntry::ById { .. } => &[],
            };
        }
        found
    }
}

/// One entry of the derived tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedEntry {
    /// A directory: an implicit prefix, a Rule 1 collision directory, a Rule 2/3
    /// `.d` spill directory, a reserved surface, or a claimant's own subtree
    /// nested inside a collision directory.
    Dir {
        name: String,
        kind: DerivedDirKind,
        children: Vec<DerivedEntry>,
    },
    /// A claimant's file entry (the root of its lens entry-set).
    Claim { name: String, claim: DerivedClaim },
    /// A dpathless document under a reserved surface (FDR 001 §5).
    ById { name: String, doc_id: DocId },
}

impl DerivedEntry {
    /// The real name of this entry inside its parent directory.
    pub fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } | Self::Claim { name, .. } | Self::ById { name, .. } => name,
        }
    }

    fn collect_paths(&self, prefix: &str, out: &mut Vec<String>) {
        let path = format!("{prefix}/{}", self.name());
        match self {
            Self::Dir { children, .. } => {
                out.push(format!("{path}/"));
                for child in children {
                    child.collect_paths(&path, out);
                }
            }
            Self::Claim { .. } | Self::ById { .. } => out.push(path),
        }
    }
}

/// Why a derived directory exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedDirKind {
    /// A prefix directory with no claim of its own (FDR 001 §3, case 5).
    Implicit,
    /// A Rule 1 collision directory: two or more claimants at one dpath.
    Collision,
    /// A Rule 2/3 `.d` spill directory.
    Spill,
    /// A claimant's own subtree, nested inside a collision directory (case 8).
    Claimant,
    /// A reserved materializer surface (FDR 001 §5).
    Reserved,
}

/// A claim as retained in the derived tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedClaim {
    pub doc_id: DocId,
    pub dpath: Dpath,
    pub facet: DpathFacet,
    pub assigned_at: Option<Timestamp>,
}

impl From<&DpathClaim> for DerivedClaim {
    fn from(claim: &DpathClaim) -> Self {
        Self {
            doc_id: claim.doc_id.clone(),
            dpath: claim.dpath.clone(),
            facet: claim.facet.clone(),
            assigned_at: claim.assigned_at,
        }
    }
}

/// A claim that contributed nothing to the tree (never a panic, never silent).
#[derive(Debug, Clone, thiserror::Error, displaydoc::Display, PartialEq, Eq)]
pub enum DpathRejection {
    /// dpath {dpath} in doc {doc_id} is not a valid dpath: {error}
    MalformedDpath {
        doc_id: DocId,
        dpath: String,
        error: DpathError,
    },
    /// doc {doc_id} declares dpath {dpath} more than once; the later claim was ignored
    DuplicateClaim { doc_id: DocId, dpath: Dpath },
}

//----------------------------------------------------------------------------
// Derivation
//----------------------------------------------------------------------------

/// Derive the real-filesystem mapping of a claimant set (FDR 001 §3).
///
/// The result is deterministic and independent of input iteration order; a
/// malformed claim contributes nothing but is reported in
/// [`DerivedTree::rejections`].
pub fn derive_tree(
    claims: impl IntoIterator<Item = RawClaim>,
    reserved: &[ReservedSurface],
    policy: &DpathNamePolicy,
) -> DerivedTree {
    let mut accepted: Vec<DpathClaim> = Vec::new();
    let mut rejections: Vec<DpathRejection> = Vec::new();
    let mut seen: BTreeSet<(DocId, Dpath)> = BTreeSet::new();

    for raw in claims {
        match raw.validate() {
            Ok(claim) => {
                if seen.insert((claim.doc_id.clone(), claim.dpath.clone())) {
                    accepted.push(claim);
                } else {
                    rejections.push(DpathRejection::DuplicateClaim {
                        doc_id: claim.doc_id,
                        dpath: claim.dpath,
                    });
                }
            }
            Err(error) => rejections.push(DpathRejection::MalformedDpath {
                doc_id: raw.doc_id,
                dpath: raw.dpath,
                error,
            }),
        }
    }

    accepted.sort_by(compare_claims);

    let ctx = Ctx {
        claims: &accepted,
        policy,
    };

    let mut trie = TrieNode::default();
    for (index, claim) in accepted.iter().enumerate() {
        trie.insert(index, &claim.dpath);
    }

    let mut wanted = dir_entries(&trie, &ctx);
    wanted.extend(reserved_entries(reserved, &ctx));
    let root = resolve(wanted, &ctx);

    DerivedTree {
        root,
        claims: accepted.iter().map(DerivedClaim::from).collect(),
        rejections,
    }
}

struct Ctx<'a> {
    claims: &'a [DpathClaim],
    policy: &'a DpathNamePolicy,
}

/// The dpath segment trie: which claims land exactly on a node, and which live
/// deeper. Segment levels are a *name* hierarchy — no claimant owns a directory
/// (FDR 001 §1: labels form a derived tree).
#[derive(Debug, Default, Clone)]
struct TrieNode {
    /// Indices into `Ctx::claims` that claim exactly this path.
    exact: Vec<usize>,
    children: BTreeMap<String, TrieNode>,
}

impl TrieNode {
    fn insert(&mut self, claim: usize, dpath: &Dpath) {
        let mut node = self;
        for segment in dpath.segments() {
            node = node.children.entry(segment.to_string()).or_default();
        }
        node.exact.push(claim);
    }

    /// Keep only the claims satisfying `keep`; `None` when nothing remains.
    fn retain(&self, keep: &dyn Fn(usize) -> bool) -> Option<Self> {
        let exact: Vec<usize> = self
            .exact
            .iter()
            .copied()
            .filter(|one| keep(*one))
            .collect();
        let mut children = BTreeMap::new();
        for (name, child) in &self.children {
            if let Some(filtered) = child.retain(keep) {
                children.insert(name.clone(), filtered);
            }
        }
        if exact.is_empty() && children.is_empty() {
            None
        } else {
            Some(Self { exact, children })
        }
    }

    /// This node's subtree without its own exact claims.
    fn without_exact(&self) -> Self {
        Self {
            exact: Vec::new(),
            children: self.children.clone(),
        }
    }

    /// Every claim index in this subtree (exact claims and deeper ones).
    fn claim_indices(&self) -> Vec<usize> {
        let mut out = self.exact.clone();
        for child in self.children.values() {
            out.extend(child.claim_indices());
        }
        out
    }
}

// Priority ranks: rule-generated entries claim names before literal ones, so a
// `.d` spill directory is awarded `/x.d` and a literal claimant of `/x.d` is
// demoted (FDR 001 §3 Rule 3, §5).
const RANK_RESERVED: u8 = 0;
const RANK_SPILL: u8 = 1;
const RANK_IMPLICIT: u8 = 2;
const RANK_COLLISION: u8 = 3;
const RANK_CLAIMANT: u8 = 4;
const RANK_LEAF: u8 = 5;

#[derive(Debug, Clone)]
struct Wanted {
    wanted: String,
    rank: u8,
    key: EntryKey,
    /// For a Rule 2 spill directory: the wanted name of the entry whose spill
    /// slot this directory occupies.
    slot_holder_of: Option<String>,
    kind: WantedKind,
}

#[derive(Debug, Clone)]
enum WantedKind {
    Leaf {
        claim: usize,
    },
    Dir {
        kind: DerivedDirKind,
        children: Vec<Wanted>,
    },
    ById {
        doc_id: DocId,
    },
}

#[derive(Debug, Clone)]
struct EntryKey {
    d_suffixes: usize,
    assigned_at: Option<Timestamp>,
    doc_id: String,
    dpath: String,
}

impl Wanted {
    fn leaf(wanted: String, claim: usize, key: EntryKey) -> Self {
        Self {
            wanted,
            rank: RANK_LEAF,
            key,
            slot_holder_of: None,
            kind: WantedKind::Leaf { claim },
        }
    }

    fn dir(
        wanted: String,
        kind: DerivedDirKind,
        children: Vec<Wanted>,
        key: EntryKey,
        slot_holder_of: Option<String>,
    ) -> Self {
        let rank = match kind {
            DerivedDirKind::Reserved => RANK_RESERVED,
            DerivedDirKind::Spill => RANK_SPILL,
            DerivedDirKind::Implicit => RANK_IMPLICIT,
            DerivedDirKind::Collision => RANK_COLLISION,
            DerivedDirKind::Claimant => RANK_CLAIMANT,
        };
        Self {
            wanted,
            rank,
            key,
            slot_holder_of,
            kind: WantedKind::Dir { kind, children },
        }
    }
}

/// FDR 001 §3, tiebreaker chain: assignment timestamp first, then lexicographic
/// object id. Unknown timestamps sort last.
fn compare_claims(one: &DpathClaim, two: &DpathClaim) -> std::cmp::Ordering {
    compare_assignment(
        one.assigned_at,
        &one.doc_id,
        &one.dpath,
        two.assigned_at,
        &two.doc_id,
        &two.dpath,
    )
}

fn compare_assignment(
    one_at: Option<Timestamp>,
    one_doc: &str,
    one_dpath: &Dpath,
    two_at: Option<Timestamp>,
    two_doc: &str,
    two_dpath: &Dpath,
) -> std::cmp::Ordering {
    compare_timestamps(one_at, two_at)
        .then_with(|| one_doc.cmp(two_doc))
        .then_with(|| one_dpath.cmp(two_dpath))
}

fn compare_timestamps(one: Option<Timestamp>, two: Option<Timestamp>) -> std::cmp::Ordering {
    match (one, two) {
        (Some(one), Some(two)) => one.cmp(&two),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn entry_key(ctx: &Ctx, indices: &[usize], wanted: &str) -> EntryKey {
    let suffix = ctx.policy.spill_suffix;
    let first = indices
        .iter()
        .copied()
        .min_by(|one, two| compare_claims(&ctx.claims[*one], &ctx.claims[*two]));
    match first {
        Some(index) => {
            let claim = &ctx.claims[index];
            EntryKey {
                d_suffixes: d_suffix_count(wanted, suffix),
                assigned_at: claim.assigned_at,
                doc_id: claim.doc_id.clone(),
                dpath: claim.dpath.to_string(),
            }
        }
        None => EntryKey {
            d_suffixes: d_suffix_count(wanted, suffix),
            assigned_at: None,
            doc_id: String::new(),
            dpath: String::new(),
        },
    }
}

fn compare_entries(one: &Wanted, two: &Wanted) -> std::cmp::Ordering {
    one.rank
        .cmp(&two.rank)
        .then_with(|| one.key.d_suffixes.cmp(&two.key.d_suffixes))
        .then_with(|| compare_timestamps(one.key.assigned_at, two.key.assigned_at))
        .then_with(|| one.key.doc_id.cmp(&two.key.doc_id))
        .then_with(|| one.key.dpath.cmp(&two.key.dpath))
        .then_with(|| one.wanted.cmp(&two.wanted))
}

/// The entries a node wants to place in its parent directory, under `segment`.
fn child_entries(node: &TrieNode, segment: &str, ctx: &Ctx) -> Vec<Wanted> {
    if node.exact.is_empty() {
        if node.children.is_empty() {
            return Vec::new();
        }
        // FDR 001 §3, case 5: a prefix-only path materializes a bare directory.
        let key = entry_key(ctx, &node.claim_indices(), segment);
        return vec![Wanted::dir(
            segment.to_string(),
            DerivedDirKind::Implicit,
            dir_entries(node, ctx),
            key,
            None,
        )];
    }

    if node.exact.len() == 1 {
        let claim = node.exact[0];
        let key = entry_key(ctx, &[claim], segment);
        let mut out = vec![Wanted::leaf(segment.to_string(), claim, key.clone())];
        if !node.children.is_empty() {
            // Rule 2: the exact claimant keeps the clean name; everything living
            // *under* that name is the divergent side and spills to `<name>.d`.
            out.push(Wanted::dir(
                ctx.policy.spill_name(segment),
                DerivedDirKind::Spill,
                dir_entries(node, ctx),
                entry_key(ctx, &node.claim_indices(), segment),
                Some(segment.to_string()),
            ));
        }
        return out;
    }

    // Rule 1: two or more claimants share the name, so the name becomes a
    // directory and every claimant gets a derived entry inside it.
    let key = entry_key(ctx, &node.claim_indices(), segment);
    vec![Wanted::dir(
        segment.to_string(),
        DerivedDirKind::Collision,
        collision_entries(node, segment, ctx),
        key,
        None,
    )]
}

/// Inside a Rule 1 collision directory: each exact claimant gets a derived entry
/// (its file, or its own subtree grouped under a derived name), and claimants
/// that only live *deeper* keep their segment names.
fn collision_entries(node: &TrieNode, segment: &str, ctx: &Ctx) -> Vec<Wanted> {
    let mut out = Vec::new();

    let exact_docs: BTreeSet<&str> = node
        .exact
        .iter()
        .map(|index| ctx.claims[*index].doc_id.as_str())
        .collect();
    for (name, child) in &node.children {
        if let Some(filtered) =
            child.retain(&|index| !exact_docs.contains(ctx.claims[index].doc_id.as_str()))
        {
            out.extend(child_entries(&filtered, name, ctx));
        }
    }

    let mut exact = node.exact.clone();
    exact.sort_by(|one, two| compare_claims(&ctx.claims[*one], &ctx.claims[*two]));
    for index in exact {
        let claim = &ctx.claims[index];
        let own = node
            .retain(&|other| ctx.claims[other].doc_id == claim.doc_id)
            .map(|node| node.without_exact());
        let identity = claimant_identity(&claim.doc_id, &claim.dpath);
        let key = entry_key(ctx, &[index], segment);
        match own {
            Some(own) if !own.children.is_empty() => {
                // Case 8: a claimant that is itself a directory gets its own
                // subtree inside the collision directory.
                out.push(Wanted::dir(
                    ctx.policy.derived_name(segment, &identity, None),
                    DerivedDirKind::Claimant,
                    dir_entries(&own, ctx),
                    key,
                    None,
                ));
            }
            _ => out.push(Wanted::leaf(
                ctx.policy
                    .derived_name(segment, &identity, claim.dpath.extension()),
                index,
                key,
            )),
        }
    }

    out
}

/// The wanted entries of a directory's children.
fn dir_entries(node: &TrieNode, ctx: &Ctx) -> Vec<Wanted> {
    let mut out = Vec::new();
    for (name, child) in &node.children {
        out.extend(child_entries(child, name, ctx));
    }
    out
}

/// The wanted entries of the reserved surfaces.
fn reserved_entries(reserved: &[ReservedSurface], ctx: &Ctx) -> Vec<Wanted> {
    reserved
        .iter()
        .map(|surface| {
            let children = match &surface.kind {
                ReservedKind::ById { doc_ids } => {
                    let mut doc_ids = doc_ids.clone();
                    doc_ids.sort();
                    doc_ids.dedup();
                    doc_ids
                        .into_iter()
                        .map(|doc_id| Wanted {
                            wanted: doc_id.clone(),
                            rank: RANK_LEAF,
                            key: EntryKey {
                                d_suffixes: 0,
                                assigned_at: None,
                                doc_id: doc_id.clone(),
                                dpath: String::new(),
                            },
                            slot_holder_of: None,
                            kind: WantedKind::ById { doc_id },
                        })
                        .collect()
                }
                ReservedKind::MetadataDir => Vec::new(),
            };
            Wanted::dir(
                surface.name.clone(),
                DerivedDirKind::Reserved,
                children,
                EntryKey {
                    d_suffixes: d_suffix_count(&surface.name, ctx.policy.spill_suffix),
                    assigned_at: None,
                    doc_id: String::new(),
                    dpath: String::new(),
                },
                None,
            )
        })
        .collect()
}

/// Assign real names to wanted entries (Rules 2 and 3).
///
/// Two rules shape the assignment:
///
/// 1. **The spill slot is reserved.** For every entry wanting a name `X`, the
///    name `X.d` belongs to that entry's divergent side (Rule 2) — so a literal
///    claimant of `X.d` is demoted to a deeper suffix, while `X`'s own spill
///    directory may occupy `X.d` (FDR 001 §3 case 4; §5 `/by-id.d/…`).
/// 2. **Contested names get deeper suffixes**, by repeatedly appending `.d`,
///    until every entry has a unique real path; the claimant whose *claimed*
///    name carries the most `.d` suffixes sorts last and loses its name first
///    (Rule 3).
///
/// Termination: each round strictly lengthens the candidate name while only
/// finitely many names are blocked, so the loop ends. A degenerate policy whose
/// spill suffix cannot lengthen a name (`""`) falls back to an identity-derived
/// name instead, so totality never depends on the policy being sane.
fn resolve(wanted: Vec<Wanted>, ctx: &Ctx) -> Vec<DerivedEntry> {
    let mut slots: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in &wanted {
        slots
            .entry(ctx.policy.spill_name(&entry.wanted))
            .or_default()
            .insert(entry.wanted.clone());
    }

    let mut ordered = wanted;
    ordered.sort_by(compare_entries);

    let mut taken: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for entry in ordered {
        let mut candidate = entry.wanted.clone();
        loop {
            let free = !taken.contains(&candidate)
                && match slots.get(&candidate) {
                    None => true,
                    Some(owners) => {
                        owners.contains(&entry.wanted)
                            || entry
                                .slot_holder_of
                                .as_ref()
                                .is_some_and(|owner| owners.contains(owner))
                    }
                };
            if free {
                break;
            }
            let next = ctx.policy.spill_name(&candidate);
            candidate = if next == candidate {
                let identity = format!("{}\u{0}{}", entry.key.doc_id, entry.key.dpath);
                let digest = identity_digest(&identity);
                let keep = ctx.policy.derived_digest_chars.min(digest.len());
                format!(
                    "{candidate}{sep}{}",
                    &digest[..keep],
                    sep = ctx.policy.derived_separator
                )
            } else {
                next
            };
        }
        taken.insert(candidate.clone());
        let Wanted {
            slot_holder_of: _,
            rank: _,
            key: _,
            wanted: _,
            kind,
        } = entry;
        out.push(build_entry(candidate, kind, ctx));
    }

    out.sort_by(|one, two| one.name().cmp(two.name()));
    out
}

fn build_entry(name: String, kind: WantedKind, ctx: &Ctx) -> DerivedEntry {
    match kind {
        WantedKind::Leaf { claim } => DerivedEntry::Claim {
            name,
            claim: DerivedClaim::from(&ctx.claims[claim]),
        },
        WantedKind::ById { doc_id } => DerivedEntry::ById { name, doc_id },
        WantedKind::Dir { kind, children } => DerivedEntry::Dir {
            name,
            kind,
            children: resolve(children, ctx),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(doc_id: &str, dpath: &str) -> RawClaim {
        RawClaim {
            doc_id: doc_id.to_string(),
            dpath: dpath.to_string(),
            facet: DpathFacet::whole_document(),
            assigned_at: None,
        }
    }

    fn derive(spec: &[(&str, &str)]) -> DerivedTree {
        derive_tree(
            spec.iter().map(|(doc_id, dpath)| raw(doc_id, dpath)),
            &[],
            &DpathNamePolicy::default(),
        )
    }

    fn paths(spec: &[(&str, &str)]) -> Vec<String> {
        derive(spec).paths()
    }

    /// The derived-name contract (FDR 001 Q1): `<stem>~<digest><.ext>`, where the
    /// digest is a function of the claimant's identity (doc id + dpath).
    fn expected_derived(
        segment: &str,
        doc_id: &str,
        dpath: &str,
        extension: Option<&str>,
    ) -> String {
        let digest = blake3::hash(format!("{doc_id}\u{0}{dpath}").as_bytes())
            .to_hex()
            .to_string();
        let stem = segment.strip_suffix(".md").unwrap_or(segment);
        match extension {
            Some(extension) => format!("{stem}~{}.{extension}", &digest[..8]),
            None => format!("{stem}~{}", &digest[..8]),
        }
    }

    fn collision(dir: &DerivedEntry) -> &[DerivedEntry] {
        match dir {
            DerivedEntry::Dir {
                kind: DerivedDirKind::Collision,
                children,
                ..
            } => children,
            other => panic!("expected a collision directory, got {other:?}"),
        }
    }

    #[test]
    fn dpath_grammar_table() {
        let accepted = [
            "/inbox/hello.md",
            "/a",
            "/a/b/c",
            "/by-id/x",
            "/.dtree/x",
            "/tagged/urgent/2026",
            "/hello world/with spaces.md",
            "/..hidden/note",
            "/café/naïve.md",
        ];
        for input in accepted {
            assert!(Dpath::parse(input).is_ok(), "should accept {input}");
        }

        let rejected: [(&str, DpathError); 7] = [
            ("", DpathError::MissingLeadingSlash { raw: String::new() }),
            (
                "inbox/hello.md",
                DpathError::MissingLeadingSlash {
                    raw: "inbox/hello.md".to_string(),
                },
            ),
            (
                "/",
                DpathError::Empty {
                    raw: "/".to_string(),
                },
            ),
            (
                "/a/",
                DpathError::TrailingSlash {
                    raw: "/a/".to_string(),
                },
            ),
            (
                "/a//b",
                DpathError::EmptySegment {
                    raw: "/a//b".to_string(),
                    index: 1,
                },
            ),
            (
                "/a/./b",
                DpathError::DotSegment {
                    raw: "/a/./b".to_string(),
                    segment: ".".to_string(),
                    index: 1,
                },
            ),
            (
                "/a/../b",
                DpathError::DotSegment {
                    raw: "/a/../b".to_string(),
                    segment: "..".to_string(),
                    index: 1,
                },
            ),
        ];
        for (input, expected) in rejected {
            assert_eq!(Dpath::parse(input).unwrap_err(), expected, "on {input}");
        }
    }

    #[test]
    fn dpath_accessors() {
        let dpath = Dpath::parse("/DCIM/2026/06/IMG_1234.jpg").unwrap();
        assert_eq!(dpath.as_str(), "/DCIM/2026/06/IMG_1234.jpg");
        assert_eq!(dpath.segment_count(), 4);
        assert_eq!(
            dpath.segments().collect::<Vec<_>>(),
            ["DCIM", "2026", "06", "IMG_1234.jpg"]
        );
        assert_eq!(dpath.file_name(), "IMG_1234.jpg");
        assert_eq!(dpath.extension(), Some("jpg"));
        assert_eq!(dpath.parent().unwrap().as_str(), "/DCIM/2026/06");
        assert_eq!(Dpath::parse("/a").unwrap().parent(), None);
        assert_eq!(Dpath::parse("/a/.hidden").unwrap().extension(), None);
        assert_eq!(
            Dpath::parse("/a/archive.tar.gz").unwrap().extension(),
            Some("gz")
        );
        assert_eq!(dpath.to_string(), dpath.as_str());
        assert_eq!("/a/b".parse::<Dpath>().unwrap().as_str(), "/a/b");
    }

    #[test]
    fn dpath_comparison_is_byte_exact() {
        // FDR 001 §1: no case folding, no Unicode normalization.
        let upper = Dpath::parse("/Inbox/Hello.md").unwrap();
        let lower = Dpath::parse("/inbox/hello.md").unwrap();
        assert_ne!(upper, lower);

        let composed = Dpath::parse("/café.md").unwrap();
        let decomposed = Dpath::parse("/cafe\u{301}.md").unwrap();
        assert_ne!(composed, decomposed);
    }

    #[test]
    fn no_reserved_namespaces_in_daybook() {
        // FDR 001 §5: reserved surfaces win their name through collisions, never
        // by forbidding labels.
        for input in ["/by-id/x", "/by-id", "/.dtree", "/.dnode/state"] {
            assert!(Dpath::parse(input).is_ok(), "should accept {input}");
        }
    }

    #[test]
    fn dpath_facet_key_round_trip() {
        let dpath = Dpath::parse("/inbox/hello.md").unwrap();
        let key = dpath.facet_key();
        assert_eq!(key.tag.to_string(), DPATH_FACET_TAG);
        assert_eq!(key.id, "/inbox/hello.md");
        // FDR 001 §2: one dpath facet per exact dpath string per doc — the key
        // renders as `<tag>//<dpath>` and splits back on the first '/'.
        assert_eq!(key.to_string(), "org.example.daybook.dpath//inbox/hello.md");

        let reparsed = FacetKey::from(key.to_string().as_str());
        assert_eq!(reparsed, key);
        assert_eq!(Dpath::parse_facet_key(&reparsed), Some(Ok(dpath.clone())));
    }

    #[test]
    fn foreign_and_malformed_facet_keys() {
        let other = FacetKey::from("org.example.daybook.blob/main");
        assert_eq!(Dpath::parse_facet_key(&other), None);

        let malformed = FacetKey::from("org.example.daybook.dpath//a//b");
        assert!(matches!(
            Dpath::parse_facet_key(&malformed),
            Some(Err(DpathError::EmptySegment { .. }))
        ));
    }

    #[test]
    fn dpath_facet_deserializes_documented_forms() {
        let whole: DpathFacet = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(whole.is_whole_document());

        let null: DpathFacet = serde_json::from_value(serde_json::json!(null)).unwrap();
        assert!(null.is_whole_document());

        let selective: DpathFacet = serde_json::from_value(serde_json::json!({
            "targets": [
                { "facetRef": "db+facet:///self/org.example.daybook.blob/main" },
                {
                    "facetRef": "db+facet:///self/org.example.daybook.imagemetadata/main",
                    "refHeads": []
                }
            ]
        }))
        .unwrap();
        assert_eq!(selective.targets.len(), 2);
        assert_eq!(selective.targets[1].ref_heads, Some(Vec::new()));
        assert!(selective.validate_targets().is_empty());

        // The single-target shorthand from the cross-doc example in FDR 001 §2.
        let shorthand: DpathFacet = serde_json::from_value(serde_json::json!({
            "facetRef": "db+facet:///other-doc/org.example.daybook.blob/main",
            "refHeads": ["h1"]
        }))
        .unwrap();
        assert_eq!(shorthand.targets.len(), 1);
        assert_eq!(shorthand.targets[0].ref_heads, Some(vec!["h1".to_string()]));

        // Unknown keys beside a known one are ignored (forward compatibility).
        let extended: DpathFacet =
            serde_json::from_value(serde_json::json!({ "targets": [], "future": 1 })).unwrap();
        assert!(extended.is_whole_document());

        // An object with neither `targets` nor `facetRef` is not a dpath facet
        // value we ever write, so it is an error rather than a silent widening
        // to whole-document scope.
        assert!(serde_json::from_value::<DpathFacet>(serde_json::json!({ "future": 1 })).is_err());
        assert!(serde_json::from_value::<DpathFacet>(serde_json::json!(42)).is_err());
    }

    #[test]
    fn dpath_facet_serializes_whole_document_as_empty_object() {
        let value = serde_json::to_value(DpathFacet::whole_document()).unwrap();
        assert_eq!(value, serde_json::json!({}));

        let selective = DpathFacet {
            targets: vec![DpathTarget {
                facet_ref: "db+facet:///self/org.example.daybook.note/main"
                    .parse()
                    .unwrap(),
                ref_heads: None,
            }],
        };
        let value = serde_json::to_value(&selective).unwrap();
        assert_eq!(
            value,
            serde_json::json!({ "targets": [
                { "facetRef": "db+facet:///self/org.example.daybook.note/main" }
            ]})
        );
        let round_tripped: DpathFacet = serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped, selective);
    }

    #[test]
    fn dpath_target_validation_flags_non_facet_refs() {
        let facet = DpathFacet {
            targets: vec![
                DpathTarget {
                    facet_ref: "https://example.com/not-a-facet".parse().unwrap(),
                    ref_heads: None,
                },
                DpathTarget {
                    facet_ref: "db+facet:///self/org.example.daybook.note/main"
                        .parse()
                        .unwrap(),
                    ref_heads: None,
                },
                // FDR 001 §2 spells its examples `db+facet://self/…`, but the
                // shipped codec requires an empty authority (`db+facet:///self/…`).
                DpathTarget {
                    facet_ref: "db+facet://self/org.example.daybook.blob/main"
                        .parse()
                        .unwrap(),
                    ref_heads: None,
                },
            ],
        };
        let issues = facet.validate_targets();
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0].index, 0);
        assert_eq!(issues[1].index, 2);
    }

    #[test]
    fn case1_single_claim_keeps_the_clean_name() {
        assert_eq!(
            paths(&[("A", "/inbox/hello.md")]),
            ["/inbox/", "/inbox/hello.md"]
        );
    }

    #[test]
    fn case2_many_claimants_make_a_collision_directory() {
        let tree = derive(&[("A", "/inbox/hello.md"), ("B", "/inbox/hello.md")]);
        let dir = tree.lookup("/inbox/hello.md").unwrap();
        let children = collision(dir);
        assert_eq!(children.len(), 2);

        let mut names: Vec<&str> = children.iter().map(DerivedEntry::name).collect();
        names.sort_unstable();
        let mut expected = [
            expected_derived("hello.md", "A", "/inbox/hello.md", Some("md")),
            expected_derived("hello.md", "B", "/inbox/hello.md", Some("md")),
        ];
        expected.sort();
        assert_eq!(
            names,
            expected.iter().map(String::as_str).collect::<Vec<_>>()
        );

        // Conflicts are opinions: both claimants are retained in full.
        let docs: BTreeSet<&str> = children
            .iter()
            .filter_map(|child| match child {
                DerivedEntry::Claim { claim, .. } => Some(claim.doc_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(docs, BTreeSet::from(["A", "B"]));

        assert_eq!(
            tree.paths(),
            vec![
                "/inbox/".to_string(),
                "/inbox/hello.md/".to_string(),
                format!(
                    "/inbox/hello.md/{}",
                    expected_derived("hello.md", "B", "/inbox/hello.md", Some("md"))
                ),
                format!(
                    "/inbox/hello.md/{}",
                    expected_derived("hello.md", "A", "/inbox/hello.md", Some("md"))
                ),
            ]
        );
    }

    #[test]
    fn case3_file_and_children_collide_into_a_spill_directory() {
        assert_eq!(
            paths(&[("A", "/hi/hello"), ("C", "/hi/hello/child")]),
            ["/hi/", "/hi/hello", "/hi/hello.d/", "/hi/hello.d/child"]
        );
    }

    #[test]
    fn case4_literal_dot_d_claim_loses_its_name() {
        // FDR 001 §3 case 4: the real path `/a.d` is the spill slot of `/a`, so
        // the object claiming the literal dpath `/a.d` is demoted.
        assert_eq!(paths(&[("A", "/a"), ("D", "/a.d")]), ["/a", "/a.d.d"]);
    }

    #[test]
    fn case5_prefix_only_paths_materialize_bare_directories() {
        let tree = derive(&[("A", "/a/b")]);
        assert_eq!(tree.paths(), ["/a/", "/a/b"]);
        assert!(matches!(
            tree.lookup("/a"),
            Some(DerivedEntry::Dir {
                kind: DerivedDirKind::Implicit,
                ..
            })
        ));
    }

    #[test]
    fn case6_rules_compose() {
        assert_eq!(
            paths(&[("A", "/a"), ("C", "/a/b"), ("D", "/a.d")]),
            ["/a", "/a.d/", "/a.d/b", "/a.d.d"]
        );
    }

    #[test]
    fn case7_dot_d_stacking_terminates_and_stacks() {
        assert_eq!(
            paths(&[("A", "/a"), ("E", "/a.d/x"), ("F", "/a.d.d")]),
            ["/a", "/a.d.d/", "/a.d.d/x", "/a.d.d.d"]
        );
    }

    #[test]
    fn case8_collision_directory_nests_claimant_subtrees() {
        let tree = derive(&[
            ("G", "/inbox"),
            ("G", "/inbox/g1"),
            ("H", "/inbox"),
            ("H", "/inbox/h1"),
        ]);
        let children = collision(tree.lookup("/inbox").unwrap());
        assert_eq!(children.len(), 2);
        for child in children {
            match child {
                DerivedEntry::Dir {
                    kind: DerivedDirKind::Claimant,
                    children,
                    ..
                } => assert_eq!(children.len(), 1),
                other => panic!("expected a claimant subtree, got {other:?}"),
            }
        }
        let docs: BTreeSet<&str> = tree
            .claims()
            .iter()
            .map(|claim| claim.doc_id.as_str())
            .collect();
        assert_eq!(docs, BTreeSet::from(["G", "H"]));
    }

    #[test]
    fn case9_reserved_surface_wins_its_literal_name() {
        let tree = derive_tree(
            [raw("A", "/by-id/x")],
            &[ReservedSurface::by_id(["dpathless-doc".to_string()])],
            &DpathNamePolicy::default(),
        );
        assert_eq!(
            tree.paths(),
            ["/by-id/", "/by-id/dpathless-doc", "/by-id.d/", "/by-id.d/x",]
        );
        assert!(matches!(
            tree.lookup("/by-id"),
            Some(DerivedEntry::Dir {
                kind: DerivedDirKind::Reserved,
                ..
            })
        ));
    }

    #[test]
    fn by_id_surface_holds_dpathless_documents_only() {
        let tree = derive_tree(
            [raw("with-dpath", "/notes/a.md")],
            &[
                ReservedSurface::by_id([
                    "z-doc".to_string(),
                    "a-doc".to_string(),
                    "a-doc".to_string(),
                ]),
                ReservedSurface::metadata_dir(".dtree"),
            ],
            &DpathNamePolicy::default(),
        );
        assert_eq!(
            tree.paths(),
            [
                "/.dtree/",
                "/by-id/",
                "/by-id/a-doc",
                "/by-id/z-doc",
                "/notes/",
                "/notes/a.md",
            ]
        );
    }

    #[test]
    fn derived_names_are_identity_derived_and_extension_preserving() {
        let tree = derive(&[("A", "/inbox/hello.md"), ("B", "/inbox/hello.md")]);
        let children = collision(tree.lookup("/inbox/hello.md").unwrap());
        for child in children {
            let DerivedEntry::Claim { name, claim } = child else {
                panic!("expected a claim entry, got {child:?}");
            };
            assert_eq!(
                *name,
                expected_derived(
                    "hello.md",
                    &claim.doc_id,
                    claim.dpath.as_str(),
                    claim.dpath.extension()
                )
            );
        }

        // A collision of directories keeps derived names free of extensions.
        let tree = derive(&[
            ("A", "/dir"),
            ("A", "/dir/a.txt"),
            ("B", "/dir"),
            ("B", "/dir/b.txt"),
        ]);
        for child in collision(tree.lookup("/dir").unwrap()) {
            let name = child.name();
            assert!(name.starts_with("dir~"), "got {name}");
            assert!(!name.ends_with(".txt"), "got {name}");
        }
    }

    #[test]
    fn claims_are_ordered_by_assignment_then_doc_id() {
        let mut late = raw("a-doc", "/notes/a.md");
        late.assigned_at = Timestamp::from_second(1_700_000_000).ok();
        let mut early = raw("z-doc", "/notes/b.md");
        early.assigned_at = Timestamp::from_second(1_600_000_000).ok();
        let unknown = raw("m-doc", "/notes/c.md");

        let tree = derive_tree(vec![late, unknown, early], &[], &DpathNamePolicy::default());
        let order: Vec<&str> = tree
            .claims()
            .iter()
            .map(|claim| claim.doc_id.as_str())
            .collect();
        // Earlier assignment first, then lexicographic doc id, unknown last.
        assert_eq!(order, ["z-doc", "a-doc", "m-doc"]);
    }

    #[test]
    fn derivation_is_input_order_independent() {
        let spec = [
            ("A", "/a"),
            ("C", "/a/b"),
            ("D", "/a.d"),
            ("E", "/a.d/x"),
            ("B", "/inbox/hello.md"),
            ("A2", "/inbox/hello.md"),
        ];
        let forward = derive(&spec);
        let mut reversed = spec;
        reversed.reverse();
        let backward = derive(&reversed);
        assert_eq!(forward.paths(), backward.paths());
        assert_eq!(forward, backward);
        assert_eq!(forward.paths(), derive(&spec).paths());
    }

    #[test]
    fn malformed_claims_are_rejected_not_silently_dropped() {
        let tree = derive(&[
            ("good", "/a/b"),
            ("bad", "relative/path"),
            ("dot", "/a/../b"),
        ]);
        assert_eq!(tree.paths(), ["/a/", "/a/b"]);
        assert_eq!(tree.claims().len(), 1);
        let rejections = tree.rejections();
        assert_eq!(rejections.len(), 2);
        assert!(matches!(
            &rejections[0],
            DpathRejection::MalformedDpath {
                error: DpathError::MissingLeadingSlash { .. },
                ..
            }
        ));
        assert!(matches!(
            &rejections[1],
            DpathRejection::MalformedDpath {
                error: DpathError::DotSegment { .. },
                ..
            }
        ));
    }

    #[test]
    fn duplicate_claims_are_reported() {
        let tree = derive(&[("A", "/a/b"), ("A", "/a/b"), ("B", "/a/b")]);
        // One facet per (doc, dpath): the duplicate is not a second opinion.
        assert_eq!(tree.claims().len(), 2);
        assert_eq!(tree.rejections().len(), 1);
        assert!(matches!(
            &tree.rejections()[0],
            DpathRejection::DuplicateClaim { doc_id, .. } if doc_id == "A"
        ));
        // `/a/b` now has two claimants, so it is a collision directory.
        assert!(tree.lookup("/a/b").is_some_and(|entry| matches!(
            entry,
            DerivedEntry::Dir {
                kind: DerivedDirKind::Collision,
                ..
            }
        )));
    }

    #[test]
    fn empty_claim_set_derives_an_empty_tree() {
        let tree = derive_tree(Vec::new(), &[], &DpathNamePolicy::default());
        assert!(tree.root().is_empty());
        assert!(tree.paths().is_empty());
        assert!(tree.claims().is_empty());
        assert!(tree.rejections().is_empty());
    }

    #[test]
    fn typed_dpath_reads_validate_while_raw_keys_report() {
        let good = Dpath::parse("/a/b.md").unwrap();
        assert_eq!(serde_json::from_str::<Dpath>("\"/a/b.md\"").unwrap(), good);
        assert_eq!(serde_json::to_string(&good).unwrap(), "\"/a/b.md\"");

        // The typed read is validated: a malformed label cannot be a `Dpath`.
        assert!(serde_json::from_str::<Dpath>("\"/a//b\"").is_err());

        // Raw facet key-ids stay tolerated and reported (FDR 001 §4).
        let key = FacetKey::from("org.example.daybook.dpath//a//b");
        assert!(matches!(
            Dpath::parse_facet_key(&key),
            Some(Err(DpathError::EmptySegment { .. }))
        ));
    }

    #[test]
    fn degenerate_name_policy_still_terminates() {
        // Totality never depends on the naming policy being sane.
        let policy = DpathNamePolicy {
            spill_suffix: "",
            ..DpathNamePolicy::default()
        };
        let tree = derive_tree([raw("A", "/a"), raw("D", "/a.d")], &[], &policy);
        let paths = tree.paths();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0], paths[1]);
    }

    #[test]
    fn deep_claims_do_not_disturb_siblings() {
        let tree = derive(&[
            ("A", "/notes"),
            ("B", "/notes"),
            ("C", "/notes/deep/er/file.md"),
            ("D", "/other.md"),
        ]);
        let children = collision(tree.lookup("/notes").unwrap());
        let names: Vec<&str> = children.iter().map(DerivedEntry::name).collect();
        assert_eq!(names.len(), 3);
        assert_eq!(
            names
                .iter()
                .filter(|name| name.starts_with("notes~"))
                .count(),
            2
        );
        // The deeper claimant only lives *under* the claimed name, so it keeps
        // its segment name inside the collision directory.
        assert!(tree.lookup("/notes/deep/er/file.md").is_some());
        assert!(tree.lookup("/other.md").is_some());
    }
}
