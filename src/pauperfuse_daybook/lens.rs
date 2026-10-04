//! Lens #1: the raw-text Note lens — the previously hardcoded
//! `Projection::select`/`Daybook::open` behavior expressed through the
//! `pauperfuse_lens` stage contract (ADR 012 §10). A whole-document claim
//! whose Body resolves to one unpinned, same-document, `text/plain` Note is
//! proposed at that Body-selected facet; every other recognizable state is a
//! recognition declination with a per-claim visible reason, never a hard
//! projector error (operator hold Q5, resolved: missing/stub/blob targets are
//! solved-visible outcomes).
//!
//! The whole-document note reverse prepare (`prepare_note_ingest`,
//! `MAX_RAW_NOTE_BYTES`) stays the lens's ingest-half contract surface; the
//! ingest coordinator keeps consuming it unchanged.

use crate::interlude::*;
use daybook_types::doc::{Blob, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag};
use daybook_types::dpath::DpathFacet;
use daybook_types::url::{FacetRef, parse_facet_ref};
use pauperfuse_lens::{
    ClaimScope, Constraint, DepAtHeads, DifferenceView, ExecCompat, FacetAccessError, FacetOp,
    FacetRole, IngestWork, LensCategory, LensDecision, LensFailure, LensIdentity, LensRegistry,
    LensVersion, OutputKind, PreparedDocOps, PreparedPlan, PlanOutput, ProjectWork, Recipe,
    RecognitionContext, Signal, SignalSet, Specificity, Subject, TargetStateClass,
    UninterpretedReason,
};
#[cfg(test)]
use pauperfuse_lens::TargetStubState;
#[cfg(test)]
use daybook_types::doc::Note;
use crate::{prepare_note_ingest, validate_note};

/// The built-in lens set of the Daybook producer surface: v1 is the raw-text
/// Note lens only. The blob lens (lens #2) registers here once its design
/// phase lands (`pf-lane-c-lens-design.md` §9).
pub fn lens_registry() -> LensRegistry {
    LensRegistry::new(vec![Box::new(RawTextNoteLens::new()), Box::new(RawBlobLens::new())])
}

/// Lens #1. v1 is unparameterized; plug-lens registration (ADR 007) names the
/// real plug later, so this identity is the stable provenance spelling.
pub struct RawTextNoteLens {
    identity: LensIdentity,
    signal_set: SignalSet,
    constraints: Vec<Constraint>,
}

impl Default for RawTextNoteLens {
    fn default() -> Self {
        Self::new()
    }
}

impl RawTextNoteLens {
    pub fn new() -> Self {
        Self {
            identity: LensIdentity {
                plug_id: "pauperfuse_daybook".into(),
                lens_name: "raw-text-note".into(),
                version: LensVersion("1".into()),
                config_digest: None,
            },
            signal_set: SignalSet::new([
                Signal::Claim { scope: ClaimScope::WholeDocument },
                Signal::FacetTag { tag: WellKnownFacetTag::Body },
            ]),
            constraints: vec![Constraint::BodySelectsNote {
                mime_types: vec!["text/plain".into()],
            }],
        }
    }

    /// The recipe this lens records for one affected (owned Note) facet, with
    /// the Body facet recorded as context, both at the evaluated heads.
    pub fn recipe(&self, affected: DepAtHeads, body_context: DepAtHeads) -> Recipe {
        Recipe {
            lens: self.identity.clone(),
            affected_inputs: vec![affected],
            context_inputs: vec![body_context],
            execution_compatibility: ExecCompat::Native,
        }
    }
}

/// A declined proposal decision: a recognition result, never an execution
/// failure (ADR 012 §5).
fn decline(reason: UninterpretedReason) -> LensDecision {
    LensDecision::Declined(reason)
}

/// The v1 whole-document claim resolution shared by the BasicFallback lenses
/// (raw-text Note, raw Blob): claim value/scope, the Body facet, its single
/// unpinned same-document reference, and the referenced target's facet value.
/// Everything here is identical for both lenses; only the target-shape match
/// at the end differs. The `Err` carries exactly the declination reason (the
/// recognition vocabulary), never an execution failure (ADR 012 §5).
struct WholeDocumentBody {
    /// The Body's facet key (read-only recipe context).
    body_key: FacetKey,
    /// The parsed Body-order[0] reference: the owned write/production target.
    reference: FacetRef,
    /// The reference spelling (for target-state surfaces).
    target_url: String,
    /// The target facet's value at the evaluated heads, `None` if absent.
    target_value: Option<serde_json::Value>,
}

fn propose_whole_document(ctx: &RecognitionContext<'_>) -> Result<WholeDocumentBody, UninterpretedReason> {
    let Subject::Dpath(dpath) = ctx.subject;
    // The claim's own facet value decides its scope. Values this codebase
    // writes are well-formed; other shapes are visible claim outcomes
    // (FDR 001 §4), not crashes.
    let Some(claim_value) = ctx.facets.get(&dpath.facet_key()) else {
        return Err(UninterpretedReason::ClaimShape(
            "dpath claim facet value is absent".into(),
        ));
    };
    let scope = match DpathFacet::from_json_value(claim_value) {
        Ok(scope) => scope,
        Err(error) => {
            return Err(UninterpretedReason::ClaimShape(format!(
                "dpath facet value: {error}"
            )));
        }
    };

    if !scope.is_whole_document() {
        // Selective claims are outside the v1 interpreter classes. The
        // targets are still accounted per claim (Q5): each target's state
        // is named, so a stub/blob/missing target stays solved-visible
        // instead of hiding behind a generic no-lens note.
        let mut states = Vec::new();
        for target in &scope.targets {
            let state = match parse_facet_ref(&target.facet_ref) {
                Err(error) => format!("malformed reference ({error:#})"),
                Ok(reference) => match reference.doc_id.as_str() {
                    doc_id if doc_id != "self" && doc_id != ctx.document.as_str() => {
                        "cross-document target (multi-document production awaits lens #3)".into()
                    }
                    _ if reference.branch.is_some() || reference.at.is_some() => {
                        "pinned target awaiting the pinned-resolution surface".into()
                    }
                    _ => TargetStateClass::of_value(ctx.facets.get(&reference.facet_key)).to_string(),
                },
            };
            states.push(format!("{}: {state}", target.facet_ref));
        }
        return Err(UninterpretedReason::NoLens(format!(
            "selective dpath claims; targets: {}",
            states.join("; ")
        )));
    }

    // Whole-document claim: resolve the Body target.
    let body_key = FacetKey::from(WellKnownFacetTag::Body);
    let Some(body_value) = ctx.facets.get(&body_key) else {
        return Err(UninterpretedReason::ClaimShape("Body facet is missing".into()));
    };
    let body = match serde_json::from_value::<WellKnownFacet>(body_value.clone()) {
        Ok(WellKnownFacet::Body(body)) => body,
        _ => {
            return Err(UninterpretedReason::ClaimShape(
                "Body facet has the wrong shape".into(),
            ));
        }
    };
    if body.order.len() != 1 {
        return Err(UninterpretedReason::ClaimShape(
            "Body must select exactly one facet".into(),
        ));
    }
    let url = &body.order[0];
    // Existing Daybook URL parsing does not percent-decode. Never silently
    // misresolve it: encoded/non-ASCII references are outside the class.
    if !url.path().is_ascii() || url.path().contains('%') || url.path().contains('\\') {
        return Err(UninterpretedReason::ReferenceUnsupported(
            "encoded/non-ASCII Body references await the facet-URL correction".into(),
        ));
    }
    let reference = match parse_facet_ref(url) {
        Ok(reference) => reference,
        Err(error) => {
            return Err(UninterpretedReason::ReferenceUnsupported(format!(
                "{error:#}"
            )));
        }
    };
    if reference.doc_id != "self" && reference.doc_id != *ctx.document {
        return Err(UninterpretedReason::ReferenceUnsupported(
            "cross-document Body reference".into(),
        ));
    }
    if reference.branch.is_some() || reference.at.is_some() {
        return Err(UninterpretedReason::ReferenceUnsupported(
            "pinned Body references".into(),
        ));
    }

    let target_value = ctx.facets.get(&reference.facet_key).cloned();
    Ok(WholeDocumentBody {
        body_key,
        target_url: url.as_str().to_string(),
        reference,
        target_value,
    })
}

/// The shared proposal construction for a resolved whole-document claim: the
/// Body-selected facet is the owned write/production dependency, the Body is
/// read-only context, and the claimed output is one file slot at the dpath's
/// derived relative path (validated at proposal time, design §2.2).
fn whole_document_proposal(
    identity: LensIdentity,
    ctx: &RecognitionContext<'_>,
    body: &WholeDocumentBody,
    reference: &FacetRef,
) -> LensDecision {
    let Subject::Dpath(dpath) = ctx.subject;
    // The claimed output path structure is validated at proposal time
    // (design §2.2: the unchanged RelPath guard, surfaced as a
    // declination instead of an error).
    let path = dpath.segments().collect::<Vec<_>>().join("/");
    let path = match pauperfuse::backends::RelPath::parse(&path) {
        Ok(rel_path) => rel_path.to_string(),
        Err(error) => return decline(UninterpretedReason::OutputPath(error.to_string())),
    };

    LensDecision::Proposed(pauperfuse_lens::Proposal {
        lens: identity,
        subject: ctx.subject.clone(),
        inputs: vec![
            // The Body-selected facet is this lens's owned write
            // destination (its ingest edit target) and its affected
            // production dependency.
            pauperfuse_lens::LensInput::Facet {
                document: ctx.document.clone(),
                facet: reference.facet_key.clone(),
                role: FacetRole::Owned,
            },
            // The Body is read-only context; it shapes the interpretation
            // and is recorded as recipe context (ADR 012 §3/§8).
            pauperfuse_lens::LensInput::Facet {
                document: ctx.document.clone(),
                facet: body.body_key.clone(),
                role: FacetRole::Context,
            },
        ],
        category: LensCategory::BasicFallback,
        specificity: Specificity(0),
        outputs: vec![pauperfuse_lens::OutputSlot {
            slot: "body".into(),
            kind: OutputKind::File,
            path,
        }],
    })
}

/// The Body's selected target value → the exact declination reason.
/// Missing/stub/blob targets are the Q5 solved-visible classifications;
/// present-but-unrepresentable states name the representation they broke.
fn body_target_reason(target: &str, value: Option<&serde_json::Value>) -> UninterpretedReason {
    match TargetStateClass::of_value(value) {
        TargetStateClass::Missing => {
            UninterpretedReason::TargetMissing(format!("Body's selected facet {target}"))
        }
        TargetStateClass::StubOrBlob(stub) => UninterpretedReason::TargetStubOrBlob {
            target: format!("Body's selected facet {target}"),
            stub,
        },
        TargetStateClass::RepresentableNoLens => UninterpretedReason::TargetShape(format!(
            "Body's selected facet {target} is not interpretable by this lens"
        )),
        TargetStateClass::NotRepresentable | TargetStateClass::UnknownShape => {
            UninterpretedReason::TargetShape("Body must select a Note facet".into())
        }
    }
}

impl pauperfuse_lens::LensInterest for RawTextNoteLens {
    fn identity(&self) -> &LensIdentity {
        &self.identity
    }

    fn signals(&self) -> &[Signal] {
        // Document-side signals only: recognition needs no file bytes.
        &self.signal_set.signals
    }

    fn constraints(&self) -> &[Constraint] {
        &self.constraints
    }
}

impl pauperfuse_lens::LensProposal for RawTextNoteLens {
    fn propose(&self, ctx: &RecognitionContext<'_>) -> LensDecision {
        let body = match propose_whole_document(ctx) {
            Ok(resolved) => resolved,
            Err(reason) => return decline(reason),
        };
        let target_note = body
            .target_value
            .as_ref()
            .and_then(|value| serde_json::from_value::<WellKnownFacet>(value.clone()).ok());
        match target_note {
            Some(WellKnownFacet::Note(note)) if note.mime == "text/plain" => {}
            Some(WellKnownFacet::Note(_)) => {
                return decline(UninterpretedReason::TargetShape(
                    "only text/plain Notes are supported".into(),
                ))
            }
            // Absent/foreign/wrong-shape targets: the Q5 outcome vocabulary.
            _ => return decline(body_target_reason(&body.target_url, body.target_value.as_ref())),
        }
        whole_document_proposal(self.identity.clone(), ctx, &body, &body.reference)
    }
}

#[async_trait]
impl pauperfuse_lens::LensPrepare for RawTextNoteLens {
    async fn prepare_ingest(&self, work: &IngestWork<'_>) -> Result<PreparedDocOps, LensFailure> {
        if work.recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                work.recipe.lens, self.identity
            )));
        }
        let affected = DepAtHeads::single_affected(work.recipe)?;
        // The candidate Note through the raw-text representation's own
        // contract: cap and encoding failures name their constraint (ADR 012
        // §6), never silent truncation or reinterpretation.
        let note = prepare_note_ingest(work.bytes)
            .map_err(|error| LensFailure::Preparation(error.to_string()))?;
        // Round-trip stability (ADR 012 §8): the unchanged representation (the
        // recorded facet value at the render heads) prepares zero operations.
        let base_note = work.base.and_then(|base| validate_note(base).ok());
        if base_note.as_ref() == Some(&note) {
            return Ok(PreparedDocOps { ops: Vec::new() });
        }
        Ok(PreparedDocOps {
            ops: vec![FacetOp {
                document: affected.document.clone(),
                facet: affected.facet.clone(),
                value: serde_json::Value::from(WellKnownFacet::Note(note)),
            }],
        })
    }

    async fn prepare_project(&self, work: &ProjectWork<'_>) -> Result<PreparedPlan, LensFailure> {
        if work.recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                work.recipe.lens, self.identity
            )));
        }
        // v1 plan: exactly one file output at the recorded path (implicit
        // parent directories are the checkout's naming surface, ADR 012 §9).
        if work.path.is_empty() {
            return Err(LensFailure::Preparation(
                "recipe records an empty output path".into(),
            ));
        }
        Ok(PreparedPlan {
            outputs: vec![PlanOutput {
                path: work.path.to_string(),
                kind: OutputKind::File,
            }],
        })
    }
}

#[async_trait]
impl pauperfuse_lens::LensProduce for RawTextNoteLens {
    async fn produce(
        &self,
        recipe: &Recipe,
        access: &dyn pauperfuse_lens::FacetAccess,
    ) -> Result<Vec<u8>, LensFailure> {
        if recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                recipe.lens, self.identity
            )));
        }
        let affected = DepAtHeads::single_affected(recipe)?;
        let Some(value) = access
            .facet_at_heads(affected)
            .await
            .map_err(|error| match error {
                FacetAccessError::Unavailable(detail) => LensFailure::Unavailable(detail),
                FacetAccessError::Runtime(detail) => LensFailure::Runtime(detail),
            })?
        else {
            return Err(LensFailure::Unavailable("selected Note is absent".into()));
        };
        // The exact render contract at the recorded heads: stale/foreign
        // facet shapes fail explicitly; nothing renders "closest" state.
        let note = validate_note(&value)
            .map_err(|error| match error {
                crate::Error::Unsupported(detail) => LensFailure::Preparation(detail),
                other => LensFailure::Runtime(other.to_string()),
            })?;
        Ok(note.content.into_bytes())
    }
}

impl pauperfuse_lens::LensDiff for RawTextNoteLens {
    fn describe_difference(
        &self,
        before: Option<&serde_json::Value>,
        after: Option<&serde_json::Value>,
    ) -> Result<DifferenceView, LensFailure> {
        // Diff view (ADR 012 §10): logical interpretation differences for
        // status and ordinary branch resolution; never conflict markers.
        let parse = |value: Option<&serde_json::Value>| value.map(validate_note);
        let (before, after) = (parse(before), parse(after));
        let mut entries = Vec::new();
        match (&before, &after) {
            (None, None) => {}
            (None, Some(Ok(_))) => entries.push("note facet added".into()),
            (Some(Ok(_)), None) => entries.push("note facet removed".into()),
            (Some(Ok(before)), Some(Ok(after))) => {
                if before.mime != after.mime {
                    entries.push(format!("note mime {} → {}", before.mime, after.mime));
                }
                if before.content != after.content {
                    entries.push(format!(
                        "note content changed ({} → {} bytes)",
                        before.content.len(),
                        after.content.len()
                    ));
                }
            }
            _ => entries.push("one side's facet state is not an interpretable Note".into()),
        }
        Ok(DifferenceView { entries })
    }
}

/// v1 inline-blob representation cap. `Blob.inline` is documented as "only to
/// be used for small blobs"; anything larger needs the chunked/fetched blob
/// backend (blob-backend ADR) and is refused until then. Revisit when that
/// ADR lands.
pub const BLOB_INLINE_CAP: usize = 1024 * 1024;

/// Lens #2: the raw Blob lens. Same BasicFallback tier as the raw-text lens —
/// FDR 004 calls text and blob the two ordinary import representations — and
/// mirror of its claim/Body-reference resolution; only the target shape
/// differs (any Blob facet, MIME display-only). Ingest is opaque bytes: no
/// decode, cap + digest contract instead.
pub struct RawBlobLens {
    identity: LensIdentity,
    signal_set: SignalSet,
    constraints: Vec<Constraint>,
}

impl Default for RawBlobLens {
    fn default() -> Self {
        Self::new()
    }
}

impl RawBlobLens {
    pub fn new() -> Self {
        Self {
            identity: LensIdentity {
                plug_id: "pauperfuse_daybook".into(),
                lens_name: "raw-blob".into(),
                version: LensVersion("1".into()),
                config_digest: None,
            },
            signal_set: SignalSet::new([
                Signal::Claim { scope: ClaimScope::WholeDocument },
                Signal::FacetTag { tag: WellKnownFacetTag::Body },
            ]),
            constraints: vec![Constraint::BodySelectsBlob],
        }
    }

    /// The recipe this lens records for one affected (owned Blob) facet, with
    /// the Body facet recorded as context, both at the evaluated heads.
    pub fn recipe(&self, affected: DepAtHeads, body_context: DepAtHeads) -> Recipe {
        Recipe {
            lens: self.identity.clone(),
            affected_inputs: vec![affected],
            context_inputs: vec![body_context],
            execution_compatibility: ExecCompat::Native,
        }
    }
}

impl pauperfuse_lens::LensInterest for RawBlobLens {
    fn identity(&self) -> &LensIdentity {
        &self.identity
    }

    fn signals(&self) -> &[Signal] {
        &self.signal_set.signals
    }

    fn constraints(&self) -> &[Constraint] {
        &self.constraints
    }
}

impl pauperfuse_lens::LensProposal for RawBlobLens {
    fn propose(&self, ctx: &RecognitionContext<'_>) -> LensDecision {
        let body = match propose_whole_document(ctx) {
            Ok(resolved) => resolved,
            Err(reason) => return decline(reason),
        };
        let target_blob = body
            .target_value
            .as_ref()
            .and_then(|value| serde_json::from_value::<WellKnownFacet>(value.clone()).ok());
        match target_blob {
            Some(WellKnownFacet::Blob(_)) => {}
            // Absent/foreign/wrong-shape targets: the Q5 outcome vocabulary.
            Some(_) => {
                return decline(UninterpretedReason::TargetShape(
                    "Body must select a Blob facet".into(),
                ))
            }
            _ => return decline(body_target_reason(&body.target_url, body.target_value.as_ref())),
        }
        whole_document_proposal(self.identity.clone(), ctx, &body, &body.reference)
    }
}

#[async_trait]
impl pauperfuse_lens::LensPrepare for RawBlobLens {
    async fn prepare_ingest(&self, work: &IngestWork<'_>) -> Result<PreparedDocOps, LensFailure> {
        if work.recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                work.recipe.lens, self.identity
            )));
        }
        let affected = DepAtHeads::single_affected(work.recipe)?;
        // The inline representation cap (§3.3): the lens recognizes and owns
        // the claim, so oversize is a preparation failure naming the real
        // constraint — never silent truncation, never a next-lens attempt.
        if work.bytes.len() > BLOB_INLINE_CAP {
            return Err(LensFailure::Preparation(format!(
                "blob of {} bytes exceeds the inline representation cap of {} bytes; \
                 chunked/fetched blob storage is not implemented yet (blob-backend ADR); \
                 nothing was staged",
                work.bytes.len(),
                BLOB_INLINE_CAP
            )));
        }
        let blob = Blob {
            // v1 has no mime sniffing: opaque bytes, honest default, surfaced
            // in status rather than guessed per format.
            mime: "application/octet-stream".into(),
            length_octets: work.bytes.len() as u64,
            digest: utils_rs::hash::hash_bytes(work.bytes),
            inline: Some(work.bytes.to_vec()),
            urls: None,
        };
        // Round-trip stability (ADR 012 §8): base equality by the byte
        // evidence (digest + length + inline), zero operations on re-read.
        if let Some(base) = work.base.and_then(|base| serde_json::from_value::<WellKnownFacet>(base.clone()).ok()) {
            let WellKnownFacet::Blob(base_blob) = base else {
                return Err(LensFailure::Preparation(
                    "recipe records a non-Blob render base".into(),
                ));
            };
            if base_blob.digest == blob.digest
                && base_blob.length_octets == blob.length_octets
                && base_blob.inline == blob.inline
            {
                return Ok(PreparedDocOps { ops: Vec::new() });
            }
        }
        Ok(PreparedDocOps {
            ops: vec![FacetOp {
                document: affected.document.clone(),
                facet: affected.facet.clone(),
                value: serde_json::Value::from(WellKnownFacet::Blob(blob)),
            }],
        })
    }

    async fn prepare_project(&self, work: &ProjectWork<'_>) -> Result<PreparedPlan, LensFailure> {
        if work.recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                work.recipe.lens, self.identity
            )));
        }
        // v1 plan: exactly one file output at the recorded path (implicit
        // parent directories are the checkout's naming surface, ADR 012 §9).
        if work.path.is_empty() {
            return Err(LensFailure::Preparation(
                "recipe records an empty output path".into(),
            ));
        }
        Ok(PreparedPlan {
            outputs: vec![PlanOutput {
                path: work.path.to_string(),
                kind: OutputKind::File,
            }],
        })
    }
}

#[async_trait]
impl pauperfuse_lens::LensProduce for RawBlobLens {
    async fn produce(
        &self,
        recipe: &Recipe,
        access: &dyn pauperfuse_lens::FacetAccess,
    ) -> Result<Vec<u8>, LensFailure> {
        if recipe.lens != self.identity {
            return Err(LensFailure::Recipe(format!(
                "recipe names {}, this lens is {}",
                recipe.lens, self.identity
            )));
        }
        let affected = DepAtHeads::single_affected(recipe)?;
        let Some(value) = access
            .facet_at_heads(affected)
            .await
            .map_err(|error| match error {
                FacetAccessError::Unavailable(detail) => LensFailure::Unavailable(detail),
                FacetAccessError::Runtime(detail) => LensFailure::Runtime(detail),
            })?
        else {
            return Err(LensFailure::Unavailable("selected Blob is absent".into()));
        };
        // The exact render contract at the recorded heads: stale/foreign
        // facet shapes fail explicitly; nothing renders "closest" state.
        let WellKnownFacet::Blob(blob) = serde_json::from_value::<WellKnownFacet>(value)
            .map_err(|error| LensFailure::Runtime(error.to_string()))?
        else {
            return Err(LensFailure::Preparation(
                "Body's selected facet is not a Blob".into(),
            ));
        };
        let Some(inline) = blob.inline else {
            return Err(LensFailure::Unavailable(
                "blob has no inline representation; chunked/fetched storage is not implemented yet (blob-backend ADR)".into(),
            ));
        };
        Ok(inline)
    }
}

impl pauperfuse_lens::LensDiff for RawBlobLens {
    fn describe_difference(
        &self,
        before: Option<&serde_json::Value>,
        after: Option<&serde_json::Value>,
    ) -> Result<DifferenceView, LensFailure> {
        // Diff view (ADR 012 §10): logical interpretation differences for
        // status and ordinary branch resolution; never conflict markers.
        let parse = |value: Option<&serde_json::Value>| {
            value.and_then(
                |value| match serde_json::from_value::<WellKnownFacet>(value.clone()) {
                    Ok(WellKnownFacet::Blob(blob)) => Some(blob),
                    _ => None,
                },
            )
        };
        let (before, after) = (parse(before), parse(after));
        let mut entries = Vec::new();
        match (&before, &after) {
            (None, None) => {}
            (None, Some(_)) => entries.push("blob facet added".into()),
            (Some(_), None) => entries.push("blob facet removed".into()),
            (Some(before), Some(after)) => {
                if before.mime != after.mime {
                    entries.push(format!("blob mime {} → {}", before.mime, after.mime));
                }
                if before.length_octets != after.length_octets {
                    entries.push(format!(
                        "blob length changed ({} → {} bytes)",
                        before.length_octets, after.length_octets
                    ));
                }
                if before.digest != after.digest {
                    let tail = |digest: &str| {
                        digest
                            .chars()
                            .rev()
                            .take(8)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect::<String>()
                    };
                    entries.push(format!(
                        "blob digest changed ({} → …{})",
                        tail(&before.digest),
                        tail(&after.digest)
                    ));
                }
            }
        }
        Ok(DifferenceView { entries })
    }
}

/// Branch-scoped facet access for production: serves exactly one facet at the
/// recorded recipe heads on the named branch surface.
pub(crate) struct BranchFacetAccess<'a> {
    drawer: &'a daybook_core::drawer::DrawerRepo,
    document: DocId,
    branch: String,
}

impl<'a> BranchFacetAccess<'a> {
    pub fn new(
        drawer: &'a daybook_core::drawer::DrawerRepo,
        document: DocId,
        branch: String,
    ) -> Self {
        Self {
            drawer,
            document,
            branch,
        }
    }
}

/// v1 access serves only the owning document: a cross-document recipe input
/// would be an availability failure, never a silent other-document read
/// (ADR 012 §7's cross-document surfaces are later lanes).
#[async_trait]
impl pauperfuse_lens::FacetAccess for BranchFacetAccess<'_> {
    async fn facet_at_heads(
        &self,
        dependency: &DepAtHeads,
    ) -> Result<Option<serde_json::Value>, FacetAccessError> {
        if dependency.document != self.document {
            return Err(FacetAccessError::Unavailable(format!(
                "recipe input names document {} but the access surface serves {}",
                dependency.document, self.document
            )));
        }
        let heads = ChangeHashSet(am_utils_rs::parse_commit_heads(&dependency.heads).map_err(
            |error| FacetAccessError::Runtime(format!("recorded recipe heads: {error}")),
        )?);
        let doc = self
            .drawer
            .get_doc_with_facets_at_branch_heads(
                &dependency.document,
                BranchPath::new(&self.branch),
                &heads,
                Some(vec![dependency.facet.clone()]),
            )
            .await
            .map_err(|error| FacetAccessError::Runtime(format!("{error:#}")))?
            .ok_or_else(|| {
                FacetAccessError::Unavailable(format!(
                    "{} at recorded recipe heads",
                    dependency.document
                ))
            })?;
        Ok(doc.facets.get(&dependency.facet).cloned())
    }
}

/// Contract-level recognition/preparation/production tests without a drawer:
/// synthetic facet contexts and a stub facet access.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAX_RAW_NOTE_BYTES;
    use daybook_types::doc::Blob;
    use std::collections::HashMap;
    use daybook_types::dpath::Dpath;
    use daybook_types::url::{FACET_SELF_DOC_ID, build_facet_ref};
    use pauperfuse_lens::{FacetAccess, FacetRole, LensDiff, LensPrepare, LensProduce};

    #[tokio::test]
    async fn whole_document_note_claim_is_proposed_and_produces() {
        let lens = RawTextNoteLens::new();
        let subject = subject("/notes/hello.md");
        let proposal = match propose(&lens, &note_facets("hello notes\n"), &subject) {
            LensDecision::Proposed(proposal) => proposal,
            decision => panic!("whole-document Note claim must propose: {decision:?}"),
        };
        assert_eq!(proposal.outputs[0].path, "notes/hello.md");
        assert_eq!(proposal.category, LensCategory::BasicFallback);
        let (document, note_facet) = proposal
            .owned_facets()
            .next()
            .expect("owned Note facet declared");
        let recipe = lens.recipe(
            DepAtHeads {
                document: document.clone(),
                facet: note_facet.clone(),
                heads: vec!["anything".into()], // the stub access ignores heads
            },
            body_context(document),
        );
        let bytes = lens
            .produce(
                &recipe,
                &StubAccess {
                    value: Some(serde_json::Value::from(WellKnownFacet::Note(Note {
                        mime: "text/plain".into(),
                        content: "hello notes\n".into(),
                    }))),
                },
            )
            .await
            .expect("production renders the Note bytes");
        assert_eq!(bytes, b"hello notes\n");
    }

    #[tokio::test]
    async fn blob_target_is_a_solved_visible_outcome() {
        // Q5 (resolved): a Body selecting a Blob facet is a classified
        // stub/blob outcome, never a hard interpretation error.
        let lens = RawTextNoteLens::new();
        let subject = subject("/media/blob.bin");
        let reason = match propose(&lens, &blob_facets(), &subject) {
            LensDecision::Declined(reason) => reason,
            decision => panic!("blob target must decline, not propose: {decision:?}"),
        };
        assert!(
            matches!(&reason, UninterpretedReason::TargetStubOrBlob { stub: TargetStubState::Blob, .. }),
            "{reason:?}"
        );
        assert!(reason.solved_visible(), "{reason}");
    }

    #[test]
    fn selective_claims_decline_with_per_target_states() {
        let lens = RawTextNoteLens::new();
        let subject = subject("/selective/thing.md");
        let mut facets = note_facets("ignored");
        let claim_value = serde_json::json!({
            "targets": [{
                "facetRef": note_ref_url().to_string(),
            }],
        });
        facets.insert(
            Dpath::parse("/selective/thing.md").unwrap().facet_key(),
            claim_value,
        );
        let decision = propose(&lens, &facets, &subject);
        match decision {
            LensDecision::Declined(UninterpretedReason::NoLens(detail)) => {
                assert!(detail.contains("selective dpath claims"), "{detail}");
            }
            _ => panic!("selective claims must decline as no-lens"),
        }
    }

    #[tokio::test]
    async fn ingest_prepares_a_note_op_and_roundtrips_to_zero_ops() {
        let lens = RawTextNoteLens::new();
        let doc: DocId = "self-doc".into();
        let note_key = FacetKey::from(WellKnownFacetTag::Note);
        let recipe = lens.recipe(
            DepAtHeads {
                document: doc.clone(),
                facet: note_key.clone(),
                heads: Vec::new(),
            },
            body_context(&doc),
        );
        let note_value = serde_json::Value::from(WellKnownFacet::Note(Note {
            mime: "text/plain".into(),
            content: "original\n".into(),
        }));
        let prepared = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes: b"edited\n",
                path: "notes/edit.md",
                base: Some(&note_value),
            })
            .await
            .expect("an edited file prepares");
        assert_eq!(prepared.ops.len(), 1);
        assert_eq!(prepared.ops[0].facet, note_key);
        prepared
            .require_declared_destinations(&proposal_for(&doc, &note_key))
            .expect("the prepared op hits the declared destination");

        // Round-trip stability: once the facet reflects the file bytes,
        // re-reading the same representation prepares zero operations.
        let edited_note = serde_json::Value::from(WellKnownFacet::Note(Note {
            mime: "text/plain".into(),
            content: "edited\n".into(),
        }));
        let same = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes: b"edited\n",
                path: "notes/edit.md",
                base: Some(&edited_note),
            })
            .await
            .expect("a re-read prepares");
        assert!(same.is_no_op(), "unchanged representation is a no-op");

        let oversized = vec![b'x'; (MAX_RAW_NOTE_BYTES as usize) + 10];
        let refused = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes: &oversized,
                path: "notes/edit.md",
                base: Some(&note_value),
            })
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("caps at"), "{refused}");
    }

    #[test]
    fn diff_reports_logical_changes_and_noops() {
        let lens = RawTextNoteLens::new();
        // Content change between two supported Notes → one entry.
        let before = serde_json::Value::from(WellKnownFacet::Note(Note {
            mime: "text/plain".into(),
            content: "a\n".into(),
        }));
        let after = serde_json::Value::from(WellKnownFacet::Note(Note {
            mime: "text/plain".into(),
            content: "b\n".into(),
        }));
        let view = lens
            .describe_difference(Some(&before), Some(&after))
            .expect("diff over Notes describes itself");
        assert_eq!(view.entries.len(), 1);
        assert!(!view.is_logical_noop());
        // An unsupported mime means the after side is no longer this lens's
        // note: the failure-model outcome is "not interpretable", one entry.
        let markdown = serde_json::Value::from(WellKnownFacet::Note(Note {
            mime: "text/markdown".into(),
            content: "b\n".into(),
        }));
        let migrated = lens
            .describe_difference(Some(&before), Some(&markdown))
            .expect("diff over a side this lens cannot interpret");
        assert_eq!(migrated.entries.len(), 1);
        assert!(migrated.entries[0].contains("not an interpretable Note"));
        let noop = lens
            .describe_difference(Some(&before), Some(&before))
            .expect("identical Notes");
        assert!(noop.is_logical_noop());
    }

    #[tokio::test]
    async fn blob_claim_is_proposed_with_a_single_file_output() {
        let lens = RawBlobLens::new();
        let subject = subject("/media/blob.bin");
        let proposal = match propose(&lens, &blob_facets(), &subject) {
            LensDecision::Proposed(proposal) => proposal,
            decision => panic!("whole-document Blob claim must propose: {decision:?}"),
        };
        assert_eq!(proposal.outputs[0].path, "media/blob.bin");
        assert_eq!(proposal.category, LensCategory::BasicFallback);
        let (document, blob_facet) = proposal
            .owned_facets()
            .next()
            .expect("owned Blob facet declared");
        assert_eq!(blob_facet, &FacetKey::from(WellKnownFacetTag::Blob));
        assert_eq!(document, &DocId::from("doc-under-test"));
    }

    #[tokio::test]
    async fn blob_ingest_prepares_an_op_and_roundtrips_to_zero_ops() {
        let lens = RawBlobLens::new();
        let doc: DocId = "self-doc".into();
        let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
        let recipe = lens.recipe(
            DepAtHeads {
                document: doc.clone(),
                facet: blob_key.clone(),
                heads: Vec::new(),
            },
            body_context(&doc),
        );
        let bytes: &[u8] = &[0, 1, 2, 3, 4, 5, 6];
        let prepared = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes,
                path: "media/blob.bin",
                base: None,
            })
            .await
            .expect("in-cap bytes prepare");
        assert_eq!(prepared.ops.len(), 1);
        assert_eq!(prepared.ops[0].facet, blob_key);
        serde_json::from_value::<WellKnownFacet>(prepared.ops[0].value.clone())
            .expect("the op value is a well-known facet");

        // The produced value is exactly ingest's representation, so re-reading
        // the same bytes prepares zero operations (round-trip stability).
        let produced = lens
            .produce(
                &recipe,
                &StubAccess {
                    value: prepared.ops[0].value.clone().into(),
                },
            )
            .await
            .expect("production returns the inline bytes");
        assert_eq!(produced, bytes);
        let same = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes,
                path: "media/blob.bin",
                base: Some(&prepared.ops[0].value),
            })
            .await
            .expect("a re-read prepares");
        assert!(same.is_no_op(), "unchanged representation is a no-op");
    }

    #[tokio::test]
    async fn blob_ingest_over_the_inline_cap_is_refused() {
        // The cap boundary: exactly cap is ingestable, one byte over is a
        // preparation failure — never silent truncation, never a next-lens
        // attempt (the lens owns the claim).
        let lens = RawBlobLens::new();
        let doc: DocId = "self-doc".into();
        let recipe = lens.recipe(
            DepAtHeads {
                document: doc,
                facet: FacetKey::from(WellKnownFacetTag::Blob),
                heads: Vec::new(),
            },
            body_context(&"self-doc".into()),
        );
        let at_cap = vec![0u8; BLOB_INLINE_CAP];
        lens.prepare_ingest(&IngestWork {
            recipe: &recipe,
            bytes: &at_cap,
            path: "media/blob.bin",
            base: None,
        })
        .await
        .expect("exactly the cap must ingest");

        let one_over = vec![0u8; BLOB_INLINE_CAP + 1];
        let refused = lens
            .prepare_ingest(&IngestWork {
                recipe: &recipe,
                bytes: &one_over,
                path: "media/blob.bin",
                base: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(refused, LensFailure::Preparation(_)), "{refused}");
        assert!(refused.to_string().contains("exceeds the inline representation cap"), "{refused}");
        assert!(
            refused.to_string().contains("chunked/fetched blob storage is not implemented"),
            "{refused}"
        );
    }

    #[tokio::test]
    async fn blob_produce_refuses_a_blob_without_an_inline_representation() {
        let lens = RawBlobLens::new();
        let doc: DocId = "self-doc".into();
        let recipe = lens.recipe(
            DepAtHeads {
                document: doc,
                facet: FacetKey::from(WellKnownFacetTag::Blob),
                heads: Vec::new(),
            },
            body_context(&"self-doc".into()),
        );
        let error = lens
            .produce(
                &recipe,
                &StubAccess {
                    value: Some(serde_json::Value::from(WellKnownFacet::Blob(Blob {
                        mime: "application/octet-stream".into(),
                        length_octets: 4,
                        digest: "sha256-000".into(),
                        inline: None,
                        urls: None,
                    }))),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, LensFailure::Unavailable(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains("chunked/fetched storage is not implemented"),
            "{error}"
        );
    }

    #[test]
    fn blob_diff_reports_mime_length_and_digest_changes() {
        let lens = RawBlobLens::new();
        let blob = |mime: &str, length: u64, digest: &str| {
            serde_json::Value::from(WellKnownFacet::Blob(Blob {
                mime: mime.into(),
                length_octets: length,
                digest: digest.into(),
                inline: Some(Vec::new()),
                urls: None,
            }))
        };
        let before = blob("application/octet-stream", 7, "sha256-aaa");
        // Same mime, only the digest differs (identical length, e.g. a rewrite).
        let digest_only = blob("application/octet-stream", 7, "sha256-bbb");
        let view = lens
            .describe_difference(Some(&before), Some(&digest_only))
            .expect("diff between Blobs describes itself");
        assert_eq!(view.entries.len(), 1);
        assert!(view.entries[0].starts_with("blob digest changed"), "{:?}", view.entries);
        // Mime + length + digest all move: three entries.
        let after = blob("image/png", 9, "sha256-bbb");
        let view = lens
            .describe_difference(Some(&before), Some(&after))
            .expect("diff between Blobs describes itself");
        assert_eq!(view.entries.len(), 3);
        let noop = lens
            .describe_difference(Some(&before), Some(&before))
            .expect("identical Blobs");
        assert!(noop.is_logical_noop());
        let removed = lens
            .describe_difference(Some(&before), None)
            .expect("removal is describable");
        assert_eq!(removed.entries[0], "blob facet removed");
    }

    // ---- fixtures ----

    fn note_facets(content: &str) -> HashMap<FacetKey, serde_json::Value> {
        let mut facets = HashMap::new();
        facets.insert(claim_key("/notes/hello.md"), serde_json::json!({}));
        facets.insert(
            FacetKey::from(WellKnownFacetTag::Body),
            serde_json::Value::from(WellKnownFacet::Body(daybook_types::doc::Body {
                order: vec![note_ref_url()],
            })),
        );
        facets.insert(
            FacetKey::from(WellKnownFacetTag::Note),
            serde_json::Value::from(WellKnownFacet::Note(Note {
                mime: "text/plain".into(),
                content: content.into(),
            })),
        );
        facets
    }

    fn blob_facets() -> HashMap<FacetKey, serde_json::Value> {
        let mut facets = HashMap::new();
        facets.insert(claim_key("/media/blob.bin"), serde_json::json!({}));
        facets.insert(
            FacetKey::from(WellKnownFacetTag::Body),
            serde_json::Value::from(WellKnownFacet::Body(daybook_types::doc::Body {
                order: vec![blob_ref_url()],
            })),
        );
        facets.insert(
            FacetKey::from(WellKnownFacetTag::Blob),
            serde_json::Value::from(WellKnownFacet::Blob(Blob {
                mime: "application/octet-stream".into(),
                length_octets: 7,
                digest: "sha256-000".into(),
                inline: Some(vec![0, 1, 2, 3, 4, 5, 6]),
                urls: None,
            })),
        );
        facets
    }

    fn blob_ref_url() -> utils_rs::prelude::Url {
        build_facet_ref(FACET_SELF_DOC_ID, &FacetKey::from(WellKnownFacetTag::Blob)).unwrap()
    }

    fn note_ref_url() -> utils_rs::prelude::Url {
        build_facet_ref(FACET_SELF_DOC_ID, &FacetKey::from(WellKnownFacetTag::Note)).unwrap()
    }

    fn claim_key(claim: &str) -> FacetKey {
        Dpath::parse(claim).unwrap().facet_key()
    }

    fn subject(claim: &str) -> Subject {
        Subject::Dpath(Dpath::parse(claim).unwrap())
    }

    fn body_context(document: &DocId) -> DepAtHeads {
        DepAtHeads {
            document: document.clone(),
            facet: FacetKey::from(WellKnownFacetTag::Body),
            heads: vec!["anything".into()],
        }
    }

    fn propose<'a>(
        lens: &'a impl pauperfuse_lens::LensProposal,
        facets: &'a HashMap<FacetKey, serde_json::Value>,
        subject: &'a Subject,
    ) -> LensDecision {
        let document: DocId = "doc-under-test".into();
        lens.propose(&RecognitionContext {
            subject,
            document: &document,
            facets,
        })
    }

    fn proposal_for(document: &DocId, note_key: &FacetKey) -> pauperfuse_lens::Proposal {
        pauperfuse_lens::Proposal {
            lens: RawTextNoteLens::new().identity,
            subject: Subject::Dpath(Dpath::parse("/notes/edit.md").unwrap()),
            inputs: vec![pauperfuse_lens::LensInput::Facet {
                document: document.clone(),
                facet: note_key.clone(),
                role: FacetRole::Owned,
            }],
            category: LensCategory::BasicFallback,
            specificity: Specificity(0),
            outputs: vec![pauperfuse_lens::OutputSlot {
                slot: "body".into(),
                kind: OutputKind::File,
                path: "notes/edit.md".into(),
            }],
        }
    }

    struct StubAccess {
        value: Option<serde_json::Value>,
    }

    #[async_trait]
    impl FacetAccess for StubAccess {
        async fn facet_at_heads(
            &self,
            _dependency: &DepAtHeads,
        ) -> Result<Option<serde_json::Value>, FacetAccessError> {
            Ok(self.value.clone())
        }
    }
}