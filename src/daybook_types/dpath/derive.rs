//! The derived real-filesystem mapping of a claimant set (FDR 001 §3).

use crate::interlude::*;

pub mod resolve;
mod trie;

#[cfg(test)]
mod tests;

use crate::doc::DocId;

use crate::dpath::claim::{DpathClaim, RawClaim};
use crate::dpath::facet::DpathFacet;
use crate::dpath::label::{Dpath, DpathError};
use crate::dpath::naming::DpathNamePolicy;

use resolve::{dir_entries, resolve};

//--------------------------------------------------------------------------------------
// The derived tree
//--------------------------------------------------------------------------------------

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
                DerivedEntry::Claim { .. } => &[],
            };
        }
        found
    }
}

/// One entry of the derived tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedEntry {
    /// A directory: an implicit prefix, a Rule 1 collision directory, a Rule 2/3
    /// `.d` spill directory, or a claimant's own subtree
    /// nested inside a collision directory.
    Dir {
        name: String,
        kind: DerivedDirKind,
        children: Vec<DerivedEntry>,
    },
    /// A claimant's file entry (the root of its lens entry-set).
    Claim { name: String, claim: DerivedClaim },
}

impl DerivedEntry {
    /// The real name of this entry inside its parent directory.
    pub fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } | Self::Claim { name, .. } => name,
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
            Self::Claim { .. } => out.push(path),
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

//--------------------------------------------------------------------------------------
// Derivation
//--------------------------------------------------------------------------------------

/// Derive the real-filesystem mapping of a claimant set (FDR 001 §3).
///
/// Only dpath claims participate here. Checkout-owned surfaces, including a
/// separate by-ID checkout, are supplied by their checkout class rather than
/// mixed into this tree.
///
/// The result is deterministic and independent of input iteration order; a
/// malformed claim contributes nothing but is reported in
/// [`DerivedTree::rejections`].
pub fn derive_tree(
    claims: impl IntoIterator<Item = RawClaim>,
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

    let mut trie = trie::TrieNode::default();
    for (index, claim) in accepted.iter().enumerate() {
        trie.insert(index, &claim.dpath);
    }

    let wanted = dir_entries(&trie, &ctx);
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
