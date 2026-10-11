//! Rule application: which real entries a node wants inside its parent, and how
//! their names are assigned.

use crate::interlude::*;

use super::trie::TrieNode;

use super::{Ctx, compare_claims};

use crate::dpath::naming::{claimant_identity, d_suffix_count, identity_digest};

use crate::dpath::reserved::{ReservedKind, ReservedSurface};

use crate::doc::DocId;

use super::{DerivedClaim, DerivedDirKind, DerivedEntry};

//--------------------------------------------------------------------------------------
// Wanted entries
//--------------------------------------------------------------------------------------

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
pub(super) struct Wanted {
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
        .then_with(|| super::compare_timestamps(one.key.assigned_at, two.key.assigned_at))
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
pub(super) fn dir_entries(node: &TrieNode, ctx: &Ctx) -> Vec<Wanted> {
    let mut out = Vec::new();
    for (name, child) in &node.children {
        out.extend(child_entries(child, name, ctx));
    }
    out
}

/// The wanted entries of the reserved surfaces.
pub(super) fn reserved_entries(reserved: &[ReservedSurface], ctx: &Ctx) -> Vec<Wanted> {
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
pub(super) fn resolve(wanted: Vec<Wanted>, ctx: &Ctx) -> Vec<DerivedEntry> {
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
