//! The dpath segment trie built by `derive_tree`.

use crate::interlude::*;

use crate::dpath::label::Dpath;

/// The dpath segment trie: which claims land exactly on a node, and which live
/// deeper. Segment levels are a *name* hierarchy — no claimant owns a directory
/// (FDR 001 §1: labels form a derived tree).
#[derive(Debug, Default, Clone)]
pub(super) struct TrieNode {
    /// Indices into `Ctx::claims` that claim exactly this path.
    pub(super) exact: Vec<usize>,
    pub(super) children: BTreeMap<String, TrieNode>,
}

impl TrieNode {
    pub(super) fn insert(&mut self, claim: usize, dpath: &Dpath) {
        let mut node = self;
        for segment in dpath.segments() {
            node = node.children.entry(segment.to_string()).or_default();
        }
        node.exact.push(claim);
    }

    /// Keep only the claims satisfying `keep`; `None` when nothing remains.
    pub(super) fn retain(&self, keep: &dyn Fn(usize) -> bool) -> Option<Self> {
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
    pub(super) fn without_exact(&self) -> Self {
        Self {
            exact: Vec::new(),
            children: self.children.clone(),
        }
    }

    /// Every claim index in this subtree (exact claims and deeper ones).
    pub(super) fn claim_indices(&self) -> Vec<usize> {
        let mut out = self.exact.clone();
        for child in self.children.values() {
            out.extend(child.claim_indices());
        }
        out
    }
}
