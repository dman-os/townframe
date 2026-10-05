//! The production recipe contract (ADR 012 §8) and the per-output provenance
//! record (design §4): a recipe names every declared input that affects the
//! output, at recorded heads, plus the execution compatibility that keeps old
//! production recipes meaningful across upgrades and future host migrations.

use daybook_types::doc::{DocId, FacetKey};
use serde::{Deserialize, Serialize};

use crate::identity::LensIdentity;
use crate::proposal::{FacetRole, LensInput, Proposal};

/// One dependency at recorded heads (ADR 012 §8: "every declared input that
/// affects that output"). Heads are serialized commit-head spellings, the
/// exact marker/vtree currency.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct DepAtHeads {
    pub document: DocId,
    pub facet: FacetKey,
    pub heads: Vec<String>,
}

impl DepAtHeads {
    /// The single-affected-input view (v1 lenses: one owned facet per output).
    /// A recipe with zero or several affected inputs is a preparation error,
    /// never silently defaulted: lenses beyond v1 read several inputs
    /// explicitly.
    pub fn single_affected(recipe: &Recipe) -> Result<&Self, super::LensFailure> {
        match recipe.affected_inputs.as_slice() {
            [only] => Ok(only),
            _ => Err(super::LensFailure::Recipe(format!(
                "recipe for {} must name exactly one affected input, has {}",
                recipe.lens,
                recipe.affected_inputs.len(),
            ))),
        }
    }
}

/// Execution compatibility (ADR 012 §10: lens version and sandbox capability
/// compatibility must be recorded so old production recipes are meaningful).
/// v1 lenses are native Rust; the WASI component id records here later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecCompat {
    /// In-process Rust implementation; the lens version pins the codec.
    Native,
}

impl core::fmt::Display for ExecCompat {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            ExecCompat::Native => "native",
        })
    }
}

/// Durable production recipe for one output: lens identity, affected inputs,
/// read-only context inputs, and execution compatibility. Deterministic per
/// declared inputs/version; a recipe change is reportable without producing a
/// digest (ADR 012 §8). Never embeds rendered bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    pub lens: LensIdentity,
    /// Inputs that affect this output (the Owned facets of the proposal),
    /// recorded at the evaluated heads.
    pub affected_inputs: Vec<DepAtHeads>,
    /// Read-only context inputs, recorded at the evaluated heads.
    pub context_inputs: Vec<DepAtHeads>,
    pub execution_compatibility: ExecCompat,
}

impl Recipe {
    /// Builds the recipe from a selected proposal at evaluated heads (design
    /// §1.4): Owned facet inputs become affected dependencies, Context facet
    /// inputs become recorded context. The heads spelling is the marker
    /// currency (serialized commit heads).
    pub fn from_proposal(proposal: &Proposal, heads: Vec<String>) -> Self {
        let facet_deps = |role: FacetRole| {
            proposal
                .inputs
                .iter()
                .filter_map(|input| match input {
                    LensInput::Facet {
                        document,
                        facet,
                        role: input_role,
                    } if *input_role == role => Some(DepAtHeads {
                        document: document.clone(),
                        facet: facet.clone(),
                        heads: heads.clone(),
                    }),
                    _ => None,
                })
                .collect()
        };
        Self {
            lens: proposal.lens.clone(),
            affected_inputs: facet_deps(FacetRole::Owned),
            context_inputs: facet_deps(FacetRole::Context),
            execution_compatibility: ExecCompat::Native,
        }
    }
}
