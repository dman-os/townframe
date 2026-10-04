//! The stateless selector (ADR 012 §4): category order, declared
//! specificity, configured-default tie-break, and the conservative
//! weakest-authority rule. It lives *outside* the lens: lenses never rank
//! themselves against competitors, enumerate competitor ids, or decide who
//! falls back.

use crate::identity::LensIdentity;
use crate::proposal::{DocumentAccess, LensInput, LensCategory, Proposal, Subject};

/// Why a proposal won its subject from the selector's viewpoint (selection is
/// stateless recompute; the reason is surfaced with the alternatives).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionReason {
    /// An explicit user override chose this lens for the subject (ADR 012 §4
    /// checkout configuration).
    ExplicitChoice,
    /// The strongest category in play, outright.
    Category,
    /// Equal category; the declared specificity rank wins.
    Specificity,
    /// Equal category and specificity; the configured default names the
    /// winner.
    ConfiguredDefault,
    /// Equal everywhere and no configured default applies: the deterministic
    /// identity ordering decides, with the alternative surfaced so it can be
    /// made explicit in configuration.
    IdentityOrder,
}

/// An explicit user choice (ADR 012 §4): the subject is matched exactly, and
/// the lens is named by its plug id and stable lens name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitChoice {
    pub subject: Subject,
    pub plug_id: String,
    pub lens_name: String,
}

/// One configured-default preference; list order is preference order (first =
/// most preferred). Entries naming no installed lens are inert hints, not
/// errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LensDefault {
    pub plug_id: String,
    pub lens_name: String,
}

/// Selection inputs (ADR 012 §4): the proposals computed this run plus the
/// checkout-local configuration (overrides and default preferences). v1 has no
/// configuration surface yet; checkout wiring adds it without changing this
/// shape.
#[derive(Debug, Clone, Default)]
pub struct SelectionConfig {
    pub explicit: Vec<ExplicitChoice>,
    pub defaults: Vec<LensDefault>,
}

/// The weakest-authority rule's result (ADR 012 §7): the effective write
/// access of a selected lens invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    /// No input demands read-only treatment; the declared destinations may
    /// receive prepared operations.
    Writable,
    /// At least one declared input is recognition-only context: the whole
    /// invocation is read-only, and filesystem permission bits reflect that
    /// (a guard, not the authority boundary — ADR 011 §7).
    ReadOnly,
}

/// ADR 012 §7: effective write access is the least permissive across every
/// declared input of the selected lens invocation, *including recognition-only
/// context*. The lens declares inputs; the selector computes the rule.
pub fn effective_authority(proposal: &Proposal) -> Authority {
    let any_read_only = proposal.inputs.iter().any(|input| {
        matches!(
            input,
            LensInput::Document { access: DocumentAccess::ReadOnlyContext, .. }
        )
    });
    if any_read_only {
        Authority::ReadOnly
    } else {
        Authority::Writable
    }
}

/// Why selection cannot produce a usable winner (proposal-integrity and
/// configuration errors). An *absence* of proposals for a subject is not an
/// error: it is the claim's uninterpreted outcome (ADR 012 §5).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SelectionError {
    /// A lens self-declared the selector-reserved category, duplicated an
    /// internal slot, or contradicted kinds/paths (ADR 012 §9 plan
    /// validation).
    #[error("invalid proposal from {lens}: {detail}")]
    InvalidProposal { lens: String, detail: String },
    /// Two winners claim the same output path across disjoint subjects (ADR
    /// 012 §9: preflight the complete plan before writing any visible member).
    #[error("proposed output path {path} collides between {left} and {right}")]
    PathCollision {
        path: String,
        left: String,
        right: String,
    },
    /// An explicit override named a lens with no proposal for its subject: the
    /// choice cannot be honored and stays visible (never silently lost).
    #[error("explicit choice {plug}/{lens} for claim {subject} produced no proposal")]
    ExplicitChoiceUnmatched {
        plug: String,
        lens: String,
        subject: String,
    },
    /// The configuration lists the same subject's explicit choice twice.
    #[error("duplicate explicit choice for claim {subject}")]
    DuplicateExplicitChoice { subject: String },
}

/// One subject's settled selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectSelection {
    pub subject: Subject,
    /// The winning proposal.
    pub winner: Proposal,
    /// Why this proposal won (surfaced with the alternatives).
    pub reason: SelectionReason,
    /// The remaining proposed interpretations, visible for status and
    /// overrides (ADR 012 §4).
    pub alternatives: Vec<Proposal>,
}

/// A complete selection: one settled winner per contested subject; disjoint
/// proposals coexist (ADR 012 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub subjects: Vec<SubjectSelection>,
}

impl Selection {
    pub fn winner(&self, subject: &Subject) -> Option<&SubjectSelection> {
        self.subjects.iter().find(|selection| &selection.subject == subject)
    }
}

/// Selects winners per subject from the proposals computed this run — and
/// nothing else. Recomputed deterministically from current state (design
/// §1.3: selection is stateless in the useful sense).
pub fn select(
    proposals: &[Proposal],
    config: &SelectionConfig,
) -> Result<Selection, SelectionError> {
    validate_proposals(proposals)?;

    let mut subjects: Vec<Subject> = proposals
        .iter()
        .map(|proposal| proposal.subject.clone())
        .collect();
    subjects.sort();
    subjects.dedup();

    for (index, choice) in config.explicit.iter().enumerate() {
        if config.explicit[index + 1..]
            .iter()
            .any(|other| other.subject == choice.subject && other.lens_name == choice.lens_name && other.plug_id == choice.plug_id)
        {
            return Err(SelectionError::DuplicateExplicitChoice {
                subject: choice.subject.to_string(),
            });
        }
    }

    let mut selections = Vec::new();
    for subject in &subjects {
        selections.push(select_subject(proposals, subject, config)?);
    }

    check_output_path_collisions(&selections)?;
    Ok(Selection { subjects: selections })
}

/// Ranks one subject's candidates and returns its settled selection. The
/// candidates came from the same subject's proposals.
fn select_subject(
    all_proposals: &[Proposal],
    subject: &Subject,
    config: &SelectionConfig,
) -> Result<SubjectSelection, SelectionError> {
    let explicit = config
        .explicit
        .iter()
        .find(|choice| &choice.subject == subject)
        .map(|choice| (
            choice,
            all_proposals.iter().position(|proposal| {
                proposal.subject == *subject
                    && proposal.lens.plug_id == choice.plug_id
                    && proposal.lens.lens_name == choice.lens_name
            }),
        ));

    if let Some((choice, None)) = &explicit {
        return Err(SelectionError::ExplicitChoiceUnmatched {
            plug: choice.plug_id.clone(),
            lens: choice.lens_name.clone(),
            subject: subject.to_string(),
        });
    }
    let is_explicit = |proposal: &Proposal| {
        explicit
            .as_ref()
            .map(|(choice, _)| {
                proposal.lens.plug_id == choice.plug_id && proposal.lens.lens_name == choice.lens_name
            })
            .unwrap_or(false)
    };

    let candidates = all_proposals.iter().filter(|proposal| proposal.subject == *subject);
    // Total ordering, strongest first (ADR 012 §4: category > declared
    // specificity > configured default > deterministic identity ordering).
    let mut candidate_ranks = candidates
        .map(|proposal| (rank(proposal, &config.defaults, is_explicit(proposal)), proposal))
        .collect::<Vec<_>>();
    candidate_ranks.sort_by(|(rank_left, _), (rank_right, _)| rank_right.cmp(rank_left));

    let winner = &candidate_ranks[0];
    let winner_rank = &winner.0;
    let reason = if winner_rank.explicit {
        SelectionReason::ExplicitChoice
    } else {
        match candidate_ranks.get(1) {
            // A single candidate: it wins by being the only interpretation in
            // its category.
            None => SelectionReason::Category,
            Some((runner_rank, _)) if runner_rank.category != winner_rank.category => {
                SelectionReason::Category
            }
            Some((runner_rank, _)) if runner_rank.specificity != winner_rank.specificity => {
                SelectionReason::Specificity
            }
            Some((runner_rank, _)) if runner_rank.default_index != winner_rank.default_index => {
                SelectionReason::ConfiguredDefault
            }
            Some((runner_rank, _)) if runner_rank.identity != winner_rank.identity => {
                SelectionReason::IdentityOrder
            }
            // Two proposals in the same category, equal specificity, same
            // default position: identical lens identities (duplicate lens
            // entries) — the registry keeps lenses distinct, so this cannot
            // happen through the contract path.
            Some(_) => unreachable!("identical lens identities proposed twice for one subject"),
        }
    };

    let alternatives = candidate_ranks[1..]
        .iter()
        .map(|(_, proposal)| (*proposal).clone())
        .collect();

    Ok(SubjectSelection {
        subject: subject.clone(),
        winner: winner.1.clone(),
        reason,
        alternatives,
    })
}

/// The total ordering key; *larger* is stronger.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct CandidateRank {
    /// Explicit choices rank above every category (ADR 012 §4).
    explicit: bool,
    category: LensCategory,
    /// Declared specificity, most specific first (design §1.3).
    specificity: u8,
    /// Configured default preference; a lower index wins, absent = last.
    default_index: usize,
    /// Deterministic `(plug_id, lens_name, version)` ordering (design §1.3).
    identity: LensIdentity,
}

fn rank(proposal: &Proposal, defaults: &[LensDefault], explicit: bool) -> CandidateRank {
    let default_index = defaults
        .iter()
        .position(|default| {
            default.plug_id == proposal.lens.plug_id && default.lens_name == proposal.lens.lens_name
        })
        .unwrap_or(usize::MAX);
    CandidateRank {
        explicit,
        category: proposal.category,
        specificity: proposal.specificity.0,
        default_index,
        identity: proposal.lens.clone(),
    }
}

/// Proposal-integrity validation (ADR 012 §9: duplicate internal editable
/// slots or contradictory kinds/paths are plan-validation errors, detected
/// before selection).
fn validate_proposals(proposals: &[Proposal]) -> Result<(), SelectionError> {
    for proposal in proposals {
        if proposal.category == LensCategory::ExplicitChoice {
            return Err(SelectionError::InvalidProposal {
                lens: proposal.lens.to_string(),
                detail: "ExplicitChoice is selector-reserved (ADR 012 §4)".into(),
            });
        }
        if proposal.outputs.is_empty() {
            return Err(SelectionError::InvalidProposal {
                lens: proposal.lens.to_string(),
                detail: "proposal declares no output slots".into(),
            });
        }
        let mut slots = proposal.outputs.clone();
        slots.sort_by(|left, right| left.slot.cmp(&right.slot));
        if let Some(duplicate) = slots.windows(2).find(|window| window[0].slot == window[1].slot) {
            return Err(SelectionError::InvalidProposal {
                lens: proposal.lens.to_string(),
                detail: format!("duplicate internal slot {}", duplicate[0].slot),
            });
        }
        for (index, slot) in proposal.outputs.iter().enumerate() {
            for other in &proposal.outputs[index + 1..] {
                if slot.path == other.path && slot.kind != other.kind {
                    let detail = format!(
                        "path {} declared with both {:?} and {:?} kinds",
                        slot.path, slot.kind, other.kind
                    );
                    return Err(SelectionError::InvalidProposal {
                        lens: proposal.lens.to_string(),
                        detail,
                    });
                }
            }
        }
    }
    Ok(())
}

/// Disjoint winners coexist; colliding claimed output paths do not (ADR 012
/// §9). One editable file never has several competing writers.
fn check_output_path_collisions(selections: &[SubjectSelection]) -> Result<(), SelectionError> {
    // Deterministic iteration (sorted subjects) fixes which proposal reports
    // first, so the error names the collision's first claim, not scan order.
    let mut claimed_by = std::collections::HashMap::<String, Subject>::new();
    for selection in selections {
        for slot in &selection.winner.outputs {
            if let Some(left) = claimed_by.remove(&slot.path) {
                return Err(SelectionError::PathCollision {
                    path: slot.path.clone(),
                    left: left.to_string(),
                    right: selection.subject.to_string(),
                });
            }
            claimed_by.insert(slot.path.clone(), selection.subject.clone());
        }
    }
    Ok(())
}