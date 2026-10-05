//! The first Daybook producer lens stack: whole-document dpath claims whose
//! Body selects a text/plain Note, routed through the `pauperfuse_lens`
//! stage contract. Recognition declinations are per-claim visible outcomes
//! (Q5); unsupported *execution* states stay explicit errors.

mod interlude {
    pub use daybook_types::doc::{
        BranchPath, ChangeHashSet, DocId, FacetKey, WellKnownFacet, WellKnownFacetTag,
    };
    pub use pauperfuse::backends::{
        BackendId, BackendTree, ByteReader, Description, OutputVersion, Producer, RelPath, Source,
        TreeEntry,
    };
    pub use utils_rs::prelude::*;
}
pub(crate) mod lens;
use crate::interlude::*;
use daybook_core::drawer::DrawerRepo;
use daybook_types::dpath::Dpath;
use std::collections::VecDeque;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported text projection: {0}")]
    Unsupported(String),
    #[error("exact Daybook source unavailable: {0}")]
    Unavailable(String),
    #[error("lens selection failed: {0}")]
    Selection(String),
    #[error("projection preparation failed: {0}")]
    Preparation(String),
    #[error(transparent)]
    Repository(#[from] eyre::Report),
}

use pauperfuse_lens::{LensPrepare, LensProduce};

pub use lens::lens_registry;
pub use lens::{RawBlobLens, RawTextNoteLens};

impl From<pauperfuse_lens::LensFailure> for Error {
    fn from(failure: pauperfuse_lens::LensFailure) -> Self {
        match failure {
            pauperfuse_lens::LensFailure::Preparation(detail) => Self::Preparation(detail),
            pauperfuse_lens::LensFailure::Unavailable(detail) => Self::Unavailable(detail),
            pauperfuse_lens::LensFailure::Runtime(detail)
            | pauperfuse_lens::LensFailure::Recipe(detail) => Self::Repository(eyre::eyre!(detail)),
        }
    }
}

/// Durable recipe for one raw-text output, with no rendered bytes.
///
/// This is the v3 marker's per-output record spelling (its `projection`,
/// `renderHeads`, and `Ready` state's length/digest are exactly the lens
/// contract's selection provenance for the single output; the marker schema
/// does not grow this phase). The contract-level types live in
/// `pauperfuse_lens` — `Recipe`, `SelectionProvenance` — and map onto these
/// fields mechanically.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub document: DocId,
    pub facet: FacetKey,
    pub path: String,
}

/// The per-claim projection selection of one document at its main heads
/// (ADR 012 §1–§5; Q5): every dpath claim gets an outcome — the selected
/// proposal with its recipe at the evaluated heads, or a visible
/// uninterpreted reason. Selection recomputes statelessly; nothing here is
/// persisted until the caller records the claim's projection.
#[derive(Debug, Clone)]
pub struct SelectedClaims {
    pub document: DocId,
    pub heads: ChangeHashSet,
    pub outcomes: Vec<pauperfuse_lens::ClaimOutcome>,
}

impl SelectedClaims {
    /// The projected claims in claim order with their recipes; today's
    /// coordinator surfaces expect exactly one.
    pub fn projected(
        &self,
    ) -> Vec<(
        pauperfuse_lens::Subject,
        Projection,
        pauperfuse_lens::Recipe,
    )> {
        self.outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                pauperfuse_lens::ClaimOutcome::Projected(projected) => {
                    let subject = &projected.subject;
                    let recipe = &projected.recipe;
                    // A projected recipe has exactly one affected (owned)
                    // input; the projection path is the first output slot's
                    // claimed structure (selector-validated non-empty).
                    let affected = recipe
                        .affected_inputs
                        .first()
                        .cloned()
                        .expect("a projected recipe has an affected input");
                    Some((
                        subject.clone(),
                        Projection {
                            document: self.document.clone(),
                            facet: affected.facet,
                            path: projected.proposal.outputs[0].path.clone(),
                        },
                        recipe.clone(),
                    ))
                }
                _ => None,
            })
            .collect()
    }
}

impl Projection {
    /// Interprets every dpath claim of the document on `main` through the
    /// lens registry: proposals → the stateless selector → per-claim
    /// outcomes. Uninterpretable claims — selective shapes, missing or
    /// stub/blob Body targets, cross-document/pinned references, malformed
    /// labels — are *outcomes*, not errors (Q5); repository/surfaces errors
    /// remain errors.
    pub async fn select_claims(
        drawer: &DrawerRepo,
        document: DocId,
    ) -> Result<SelectedClaims, Error> {
        let (doc, heads) = drawer
            .get_with_heads(&document, BranchPath::new("main"), None)
            .await?
            .ok_or_else(|| Error::Unavailable(format!("document {document} on main")))?;
        let heads_wire = am_utils_rs::serialize_commit_heads(&heads);
        let registry = lens_registry();
        let config = pauperfuse_lens::SelectionConfig::default();

        // Deterministic claim order (sorted labels); malformed labels stay
        // enumerated and visible (FDR 001 §4: reported, never silently
        // dropped).
        let mut claims = doc
            .facets
            .keys()
            .filter(|key| Dpath::parse_facet_key(key).is_some())
            .collect::<Vec<_>>();
        claims.sort_by(|left, right| left.id.cmp(&right.id));

        let mut outcomes = Vec::new();
        for key in claims {
            let dpath = match Dpath::parse_facet_key(key) {
                Some(Ok(dpath)) => dpath,
                Some(Err(error)) => {
                    outcomes.push(pauperfuse_lens::ClaimOutcome::Uninterpreted(
                        pauperfuse_lens::Uninterpreted {
                            subject: None,
                            reason: pauperfuse_lens::UninterpretedReason::ClaimMalformed(format!(
                                "{key}: {error}"
                            )),
                        },
                    ));
                    continue;
                }
                None => unreachable!("filter kept dpath facets only"),
            };
            let subject = pauperfuse_lens::Subject::Dpath(dpath.clone());
            let ctx = pauperfuse_lens::RecognitionContext {
                subject: &subject,
                document: &document,
                facets: &doc.facets,
            };
            let mut proposed = Vec::new();
            let mut declined = Vec::new();
            for registered in registry.lenses() {
                match registered.propose(&ctx) {
                    pauperfuse_lens::LensDecision::Proposed(proposal) => proposed.push(proposal),
                    pauperfuse_lens::LensDecision::Declined(reason) => declined.push(reason),
                }
            }
            if proposed.is_empty() {
                let reason =
                    pauperfuse_lens::combine_declinations(&declined).unwrap_or_else(|| {
                        pauperfuse_lens::UninterpretedReason::NoLens("no lens is installed".into())
                    });
                outcomes.push(pauperfuse_lens::ClaimOutcome::Uninterpreted(
                    pauperfuse_lens::Uninterpreted {
                        subject: Some(subject),
                        reason,
                    },
                ));
                continue;
            }
            let selection = pauperfuse_lens::select(&proposed, &config)
                .map_err(|error| Error::Selection(error.to_string()))?;
            let subject_selection = selection
                .winner(&subject)
                .expect("a subject with proposals has a winner");
            outcomes.push(pauperfuse_lens::ClaimOutcome::Projected(Box::new(
                pauperfuse_lens::ProjectedClaim {
                    subject,
                    proposal: subject_selection.winner.clone(),
                    recipe: pauperfuse_lens::Recipe::from_proposal(
                        &subject_selection.winner,
                        heads_wire.clone(),
                    ),
                },
            )));
        }
        Ok(SelectedClaims {
            document,
            heads,
            outcomes,
        })
    }
}

/// Upper bound for the raw-text Note representation, in bytes. Note content
/// lives inline inside the facet value, so a runaway file would bloat the
/// Automerge document itself rather than a blob store; oversized files are an
/// explicit preparation failure, never a silent truncation.
pub const MAX_RAW_NOTE_BYTES: u64 = 4 * 1024 * 1024;

/// Raw-text Note lens inverse (ADR 012 §6): UTF-8 file bytes become the
/// candidate Note facet for the bound output slot. Oversized and non-UTF-8
/// files are preparation failures naming the constraint; bytes are never
/// silently ignored or reinterpreted.
pub fn prepare_note_ingest(bytes: &[u8]) -> Result<daybook_types::doc::Note, Error> {
    let length = bytes.len() as u64;
    if length > MAX_RAW_NOTE_BYTES {
        return Err(Error::Unsupported(format!(
            "file is {length} bytes; the raw-text Note representation caps at {MAX_RAW_NOTE_BYTES}"
        )));
    }
    let content = std::str::from_utf8(bytes).map_err(|error| {
        Error::Unsupported(format!(
            "file is not valid UTF-8; the raw-text Note representation has no other encoding (byte {}): {error}",
            error.valid_up_to()
        ))
    })?;
    Ok(daybook_types::doc::Note {
        mime: "text/plain".into(),
        content: content.into(),
    })
}

/// Validates a facet value is exactly the supported representation: a
/// text/plain Note. Ingest reads recorded facets at rendered heads through
/// this guard so stale or foreign facet shapes fail explicitly.
pub fn validate_note(value: &serde_json::Value) -> Result<daybook_types::doc::Note, Error> {
    let WellKnownFacet::Note(note) = serde_json::from_value(value.clone())? else {
        return Err(Error::Unsupported("Body must select a Note facet".into()));
    };
    if note.mime != "text/plain" {
        return Err(Error::Unsupported(
            "only text/plain Notes are supported".into(),
        ));
    }
    Ok(note)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Output {
    document: DocId,
    facet: FacetKey,
    branch: String,
}

/// A lens-driven producer over an independently owned checkout branch: its
/// observation is the selected proposal's output plan, and its reads go
/// through the lens's production stage at exactly the recorded recipe heads.
pub struct Daybook {
    drawer: Arc<DrawerRepo>,
    id: BackendId,
    projection: Projection,
    branch: String,
    heads: ChangeHashSet,
    lens: lens::RawTextNoteLens,
    recipe: pauperfuse_lens::Recipe,
}

impl Daybook {
    pub fn new(
        drawer: Arc<DrawerRepo>,
        id: BackendId,
        projection: Projection,
        branch: String,
        heads: ChangeHashSet,
    ) -> Self {
        let current = lens::RawTextNoteLens::new();
        let heads_wire = am_utils_rs::serialize_commit_heads(&heads);
        let affected = pauperfuse_lens::DepAtHeads {
            document: projection.document.clone(),
            facet: projection.facet.clone(),
            heads: heads_wire.clone(),
        };
        // The Body is the claim's read-only context dependency at the same
        // heads (ADR 012 §8: every declared input recorded).
        let context = pauperfuse_lens::DepAtHeads {
            document: projection.document.clone(),
            facet: FacetKey::from(WellKnownFacetTag::Body),
            heads: heads_wire,
        };
        let recipe = current.recipe(affected, context);
        Self {
            drawer,
            id,
            projection,
            branch,
            heads,
            lens: current,
            recipe,
        }
    }
    fn output(&self) -> Output {
        Output {
            document: self.projection.document.clone(),
            facet: self.projection.facet.clone(),
            branch: self.branch.clone(),
        }
    }
    pub fn source(&self) -> Source {
        Source {
            backend: self.id.clone(),
            output: OutputVersion {
                output: serde_json::to_vec(&self.output()).unwrap(),
                version: serde_json::to_vec(&am_utils_rs::serialize_commit_heads(&self.heads))
                    .unwrap(),
            },
        }
    }
}

pub struct Observation(VecDeque<TreeEntry>);
impl BackendTree for Observation {
    type Error = Error;
    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        Ok(self.0.pop_front())
    }
}

pub struct Reader(Vec<u8>);
impl ByteReader for Reader {
    type Error = Error;
    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        let bytes = self.0.get(offset..).unwrap_or_default();
        let length = bytes.len().min(buffer.len());
        buffer[..length].copy_from_slice(&bytes[..length]);
        Ok(length)
    }
}

impl Producer for Daybook {
    type Error = Error;
    type Tree = Observation;
    type Reader = Reader;
    fn id(&self) -> &BackendId {
        &self.id
    }
    async fn observe(&self) -> Result<Self::Tree, Self::Error> {
        // The complete output plan comes from the lens (ADR 012 §9: paths/
        // kinds declared before visible publication); the producer adds the
        // identity selector and materializes the ancestor chains.
        let plan = self
            .lens
            .prepare_project(&pauperfuse_lens::ProjectWork {
                recipe: &self.recipe,
                path: &self.projection.path,
            })
            .await?;
        let mut entries = Vec::new();
        for output in &plan.outputs {
            let path = RelPath::parse(&output.path)
                .map_err(|error| Error::Unsupported(error.to_string()))?;
            match output.kind {
                pauperfuse_lens::OutputKind::File => {
                    entries.extend(path.ancestors_inclusive().take(path.len()).map(|path| {
                        TreeEntry {
                            path,
                            description: Description::Directory,
                        }
                    }));
                    entries.push(TreeEntry {
                        path,
                        description: Description::File {
                            source: self.source(),
                            size: None,
                        },
                    });
                }
                pauperfuse_lens::OutputKind::Directory => {
                    entries.push(TreeEntry {
                        path,
                        description: Description::Directory,
                    });
                }
            }
        }
        Ok(Observation(entries.into()))
    }
    async fn open(&self, selector: &OutputVersion) -> Result<Self::Reader, Self::Error> {
        let output: Output = serde_json::from_slice(&selector.output)?;
        if output != self.output() {
            return Err(Error::Unavailable(
                "output does not belong to this projection".into(),
            ));
        }
        // Production serves exactly the selector's version (never the latest):
        // the recipe's recorded heads must be the selected ones, or the recipe
        // provenance is broken (never silently re-rendered elsewhere).
        let heads: Vec<String> = serde_json::from_slice(&selector.version)?;
        let affected = self
            .recipe
            .affected_inputs
            .first()
            .expect("Daybook recipes have one affected input");
        if affected.heads != heads {
            return Err(Error::Unavailable(
                "selected version does not match the recorded recipe heads".into(),
            ));
        }
        let access = lens::BranchFacetAccess::new(
            &self.drawer,
            self.projection.document.clone(),
            self.branch.clone(),
        );
        let bytes = self.lens.produce(&self.recipe, &access).await?;
        Ok(Reader(bytes))
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Repository(error.into())
    }
}

// ---- raw-text Note lens inverse (used by checkout ingest) ----

#[cfg(test)]
mod ingest_tests {
    use super::*;

    #[test]
    fn utf8_bytes_become_a_text_plain_note() {
        let note = prepare_note_ingest(b"hello notes\nsecond\n").unwrap();
        assert_eq!(note.mime, "text/plain");
        assert_eq!(note.content, "hello notes\nsecond\n");
    }

    #[test]
    fn non_utf8_files_are_explicit_preparation_failures() {
        let error = prepare_note_ingest(&[0xC3, 0x28, b'a']).unwrap_err();
        assert!(error.to_string().contains("not valid UTF-8"), "{error}");
    }

    #[test]
    fn oversized_files_are_refused_before_decoding() {
        // The length guard fires before UTF-8 decoding, so invalid bytes above
        // the cap still name the cap, not only the encoding failure.
        let bytes = [0xC3u8, 0x28].repeat((MAX_RAW_NOTE_BYTES as usize) / 2 + 1);
        let error = prepare_note_ingest(&bytes).unwrap_err();
        assert!(error.to_string().contains("caps at"), "{error}");
    }

    #[test]
    fn empty_files_are_a_legal_raw_text_note() {
        let note = prepare_note_ingest(b"").unwrap();
        assert_eq!(note.content, "");
    }
}
