//! The read and write gate: is facet X at heads H exactly what valid signatures say it is?
//!
//! Every change that modifies a facet's content also introduces a signature on that facet, and the
//! signature covers the facet's value as that change left it. The facet's **facet heads** are the
//! introductions of the signatures currently covering it. X at H is accepted when:
//!
//! 1. **The current signatures verify.** Each claim, reconstructed at its introduction, matches the
//!    signed bytes, and the actor's attribution certificate verifies against the node that made
//!    the change. A proven-invalid current entry rejects that facet.
//! 2. **Nothing touched X outside them.** Every change in H's history that touched X and is not
//!    already beneath the facet heads must itself introduce a valid signature on X. A change that
//!    touched X without signing it — even one whose value lost an Automerge conflict and is not
//!    visible at H — is a forgery and rejects the facet.
//! 3. **The value is what they signed.** The value at H equals the value at the facet heads, i.e.
//!    the signed values merged by Automerge's own rules. This stops signature entries from being
//!    deleted out from under a value.
//!
//! Changes beneath the facet heads are not re-examined: whoever signed on top of them passed this
//! same gate for their base first. That is why a hidden unsigned contribution cannot be laundered.
//! An honest writer's base check sees it (it is not beneath the signatures that existed then) and
//! refuses to sign; only a writer bypassing the gate could sign over it, and their signature is
//! then theirs to answer for. It is also what keeps a slot recoverable: an explicit repair signs a
//! new value and the old forgery falls beneath it.
//!
//! Pending means verification could not complete; it never means part of an applied change may
//! arrive later. Missing document metadata or attribution certificates are malformed, not pending.
//! Checks are facet-local: a forgery in another facet changes nothing here.

use super::claim_history::{
    ClaimHistoryError, ClaimHistoryResult, HistoryBudget, ReconstructedClaim, reconstruct_claim,
    reconstruct_current_claims,
};
use automerge::legacy::{Key, ObjectId, OpId, OpType};
use automerge::{ActorId, Automerge, ChangeHash, ObjType, ReadDoc, Value};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ActorMetadata, Attribution, AttributionError, ClaimOperation, DMETA_FACET_TAG, DocumentId,
    MultibaseKey, NativeReadBudget, NativeValue, cid_of, encode_native,
};
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) const DMETA_KEY: &str = "org.example.daybook.dmeta/main";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Verdict {
    Accepted {
        facet_heads: Vec<ChangeHash>,
        native: Option<Vec<u8>>,
    },
    Pending {
        reason: String,
        facet_heads: Vec<ChangeHash>,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Default)]
struct Footprint {
    touched: Vec<ChangeHash>,
    signed: HashSet<ChangeHash>,
}

pub(super) struct Gate<'a> {
    doc: &'a Automerge,
    heads: &'a [ChangeHash],
    footprints: HashMap<String, Footprint>,
    envelope_changes: Vec<ChangeHash>,
    budget: HistoryBudget,
}

enum Role {
    Facets,
    Slot(String),
    Dmeta,
    DmetaFacets,
    FacetMeta(String),
    Signatures(String),
}

impl<'a> Gate<'a> {
    /// One pass over H's history records, per facet key, which changes touched the facet's
    /// content and which introduced a signature on it.
    pub(super) fn new(doc: &'a Automerge, heads: &'a [ChangeHash]) -> Self {
        let ancestry = ancestry(doc, heads.iter().copied());
        let mut roles: HashMap<OpId, Role> = HashMap::new();
        let mut footprints: HashMap<String, Footprint> = HashMap::new();
        let mut envelope_changes = Vec::new();
        for change in doc.get_changes(&[]) {
            let hash = change.hash();
            if !ancestry.contains(&hash) {
                continue;
            }
            let decoded = change.decode();
            for (index, op) in decoded.operations.iter().enumerate() {
                let id = OpId(
                    decoded.start_op.get() + index as u64,
                    decoded.actor_id.clone(),
                );
                let made = matches!(op.action, OpType::Make(_));
                let key = match &op.key {
                    Key::Map(key) => Some(key.as_str()),
                    Key::Seq(_) => None,
                };
                let shared = match (&op.obj, key) {
                    (ObjectId::Root, Some("facets")) => true,
                    (ObjectId::Id(parent), Some(key)) => matches!(
                        (roles.get(parent), key),
                        (Some(Role::Facets), DMETA_KEY) | (Some(Role::Dmeta), "facets")
                    ),
                    _ => false,
                };
                if shared && envelope_changes.last() != Some(&hash) {
                    envelope_changes.push(hash);
                }
                let role = match &op.obj {
                    ObjectId::Root => match (key, &op.action) {
                        (Some("facets"), OpType::Make(ObjType::Map)) => Some(Role::Facets),
                        _ => None,
                    },
                    ObjectId::Id(parent) => match roles.get(parent) {
                        None => None,
                        Some(Role::Facets) => match key {
                            Some(DMETA_KEY) => matches!(op.action, OpType::Make(ObjType::Map))
                                .then_some(Role::Dmeta),
                            Some(slot) => {
                                touch(&mut footprints, slot, hash);
                                made.then(|| Role::Slot(slot.to_owned()))
                            }
                            None => None,
                        },
                        Some(Role::Slot(slot)) => {
                            let slot = slot.clone();
                            touch(&mut footprints, &slot, hash);
                            made.then_some(Role::Slot(slot))
                        }
                        Some(Role::Dmeta) => (key == Some("facets")
                            && matches!(op.action, OpType::Make(ObjType::Map)))
                        .then_some(Role::DmetaFacets),
                        Some(Role::DmetaFacets) => match key {
                            Some(slot) => {
                                touch(&mut footprints, slot, hash);
                                matches!(op.action, OpType::Make(ObjType::Map))
                                    .then(|| Role::FacetMeta(slot.to_owned()))
                            }
                            None => None,
                        },
                        Some(Role::FacetMeta(slot)) => {
                            if key == Some("signatures") {
                                touch(&mut footprints, slot, hash);
                                matches!(op.action, OpType::Make(ObjType::List))
                                    .then(|| Role::Signatures(slot.clone()))
                            } else {
                                None
                            }
                        }
                        Some(Role::Signatures(slot)) => {
                            touch(&mut footprints, slot, hash);
                            if op.insert && matches!(op.action, OpType::Make(ObjType::Map)) {
                                footprints
                                    .entry(slot.clone())
                                    .or_default()
                                    .signed
                                    .insert(hash);
                            }
                            None
                        }
                    },
                };
                if let Some(role) = role {
                    roles.insert(id, role);
                }
            }
        }
        Self {
            doc,
            heads,
            footprints,
            envelope_changes,
            budget: HistoryBudget::default(),
        }
    }

    /// Every facet key touched in `heads`'s history, including slots later deleted.
    pub(super) fn keys(&self) -> impl Iterator<Item = FacetKey> + '_ {
        self.footprints.keys().filter_map(|key| key.parse().ok())
    }

    pub(super) fn verify(&self, key: &FacetKey) -> Verdict {
        assert_ne!(
            key.tag().as_str(),
            DMETA_FACET_TAG,
            "dmeta is not a signed facet"
        );
        let mut pending: Option<String> = None;
        let entries = match reconstruct_current_claims(self.doc, key, self.heads, self.budget) {
            Ok(entries) => entries,
            Err(error) => return unreadable(error),
        };
        let mut facet_heads = BTreeSet::new();
        for entry in entries {
            let introduction = self
                .doc
                .hash_for_opid(&entry.marker)
                .expect("an observed operation belongs to an applied change");
            match self.claim_status(entry.result) {
                Status::Valid => {
                    facet_heads.insert(introduction);
                }
                Status::Pending(reason) => {
                    facet_heads.insert(introduction);
                    pending.get_or_insert(reason);
                }
                Status::Invalid(reason) => return Verdict::Rejected { reason },
            }
        }
        if let Some(footprint) = self.footprints.get(&key.to_string()) {
            let covered = ancestry(self.doc, facet_heads.iter().copied());
            for change in footprint
                .touched
                .iter()
                .chain(&self.envelope_changes)
                .filter(|change| !covered.contains(change))
            {
                if !footprint.signed.contains(change) {
                    return Verdict::Rejected {
                        reason: format!("change {change} modified the facet without signing it"),
                    };
                }
                match self.claim_status(reconstruct_claim(self.doc, key, *change, self.budget)) {
                    Status::Valid => {}
                    Status::Pending(reason) => {
                        pending.get_or_insert(reason);
                    }
                    Status::Invalid(reason) => {
                        return Verdict::Rejected {
                            reason: format!(
                                "change {change} carries an invalid signature: {reason}"
                            ),
                        };
                    }
                }
            }
        }
        let facet_heads: Vec<_> = facet_heads.into_iter().collect();
        let current = match self.native(key, self.heads) {
            Ok(native) => native,
            Err(error) => return unreadable(error),
        };
        let signed = if facet_heads.is_empty() {
            None
        } else {
            match self.native(key, &facet_heads) {
                Ok(native) => native,
                Err(error) => return unreadable(error),
            }
        };
        if current != signed {
            return Verdict::Rejected {
                reason: "the value is not the one its signatures cover".into(),
            };
        }
        match pending {
            Some(reason) => Verdict::Pending {
                reason,
                facet_heads,
            },
            None => Verdict::Accepted {
                facet_heads,
                native: current,
            },
        }
    }

    fn native(
        &self,
        key: &FacetKey,
        heads: &[ChangeHash],
    ) -> Result<Option<Vec<u8>>, ClaimHistoryError> {
        let Some((value, facets)) = self.doc.get_at(automerge::ROOT, "facets", heads)? else {
            return Ok(None);
        };
        if value != Value::Object(ObjType::Map) {
            return Err(ClaimHistoryError::MalformedWitness("facets map"));
        }
        let mut budget = NativeReadBudget {
            nodes: self.budget.max_value_nodes,
            bytes: self.budget.max_bytes,
            max_depth: self.budget.max_depth,
        };
        Ok(
            NativeValue::read_at_heads(self.doc, &facets, key.to_string(), heads, &mut budget)?
                .map(|native| encode_native(&native)),
        )
    }

    fn claim_status(&self, result: ClaimHistoryResult) -> Status {
        match result {
            ClaimHistoryResult::Accepted(claim) => {
                if claim.signature.inherited.is_some() {
                    let native = match self
                        .native(&claim.claim.facet_key, &claim.introduction.resulting_heads)
                    {
                        Ok(native) => native,
                        Err(error) => {
                            return match error {
                                ClaimHistoryError::ResourceRefused(_)
                                | ClaimHistoryError::Unsupported(_) => {
                                    Status::Pending(describe(&error))
                                }
                                ClaimHistoryError::MalformedWitness(_)
                                | ClaimHistoryError::InvalidSignature
                                | ClaimHistoryError::Automerge(_) => {
                                    Status::Invalid(describe(&error))
                                }
                            };
                        }
                    };
                    let covered = match (claim.claim.operation, native) {
                        (ClaimOperation::Set, Some(native)) => {
                            claim.claim.value_cid == Some(cid_of(&native))
                        }
                        (ClaimOperation::Remove, None) => true,
                        _ => false,
                    };
                    if !covered {
                        return Status::Invalid(
                            "the inherited claim does not cover the imported value".into(),
                        );
                    }
                }
                attribution(&claim)
            }
            ClaimHistoryResult::Pending(error) => Status::Pending(describe(&error)),
            ClaimHistoryResult::ProvenForgery(error) => Status::Invalid(describe(&error)),
        }
    }
}

enum Status {
    Valid,
    Pending(String),
    Invalid(String),
}

/// The given changes and everything they depend on.
fn ancestry(doc: &Automerge, heads: impl IntoIterator<Item = ChangeHash>) -> HashSet<ChangeHash> {
    let mut seen = HashSet::new();
    let mut pending: Vec<ChangeHash> = heads.into_iter().collect();
    while let Some(hash) = pending.pop() {
        if seen.insert(hash) {
            let change = doc
                .get_change_by_hash(&hash)
                .expect("the gate runs on heads the caller checked are known");
            pending.extend(change.deps().iter().copied());
        }
    }
    seen
}

fn touch(footprints: &mut HashMap<String, Footprint>, slot: &str, hash: ChangeHash) {
    let touched = &mut footprints.entry(slot.to_owned()).or_default().touched;
    if touched.last() != Some(&hash) {
        touched.push(hash);
    }
}

/// Check certificate consistency with the node key claimed by the introducing change's actor.
/// These actor bytes do not authenticate the substrate signer or establish Keyhive write authority.
fn attribution(claim: &ReconstructedClaim) -> Status {
    let Some(issuer) = node_key(&claim.introduction.automerge_actor) else {
        return Status::Invalid("the change's actor is not a node key".into());
    };
    let (Some(actor_url), Some(certificate)) = (&claim.actor_url, &claim.certificate) else {
        return Status::Invalid(
            "the actor has no attribution certificate or URL at introduction".into(),
        );
    };
    let actor = ActorMetadata {
        pubkey: claim.actor_public_key.clone(),
        actor_url: actor_url.clone(),
        delegation: Some(certificate.clone()),
    };
    match Attribution::verify(certificate, &issuer, claim.claim.actor_uuid, &actor) {
        Ok(_) => Status::Valid,
        Err(error @ (AttributionError::Unsupported(_) | AttributionError::ResourceLimit)) => {
            Status::Pending(format!("attribution: {error}"))
        }
        Err(error) => Status::Invalid(format!("attribution: {error}")),
    }
}

/// Automerge prefix of an actor it derived to write at historical heads (`ActorId::with_concurrency`
/// in automerge 0.11): magic bytes, a LEB128 level, then the base actor's bytes.
const CONCURRENCY_MAGIC: [u8; 4] = [0x13, 0xb2, 0x23, 0x09];
const DOCUMENT_TAG_BYTES: usize = 16;

/// The Automerge actor DocumentsRepo authors a document's changes under: the node key, then the
/// first 16 bytes of the BLAKE3 hash of the document id.
///
/// The node key is what a verifier reads back to find the expected issuer of the attribution
/// certificates a change carries. The document tag is there because Automerge identifies a change
/// by its actor and sequence number: a branch starts as its origin's changes, and if both were then
/// written under the bare node key, each would hold a different change at the same sequence number
/// and merging the branch back would be refused.
pub(super) fn document_actor(node: &MultibaseKey, document: &DocumentId) -> ActorId {
    let mut bytes = node.as_bytes().to_vec();
    bytes.extend_from_slice(&blake3::hash(document.as_bytes()).as_bytes()[..DOCUMENT_TAG_BYTES]);
    ActorId::from(bytes)
}

/// The node key a change was authored under ([`document_actor`]). A write at historical heads makes
/// Automerge derive a concurrency actor, which still ends in the same base actor's bytes.
fn node_key(actor: &ActorId) -> Option<MultibaseKey> {
    let bytes = actor.to_bytes();
    let base = match bytes.strip_prefix(CONCURRENCY_MAGIC.as_slice()) {
        None => bytes,
        Some(rest) => {
            let level = rest.iter().position(|byte| byte & 0x80 == 0)?;
            &rest[level + 1..]
        }
    };
    let (key, tag) = base.split_at_checked(32)?;
    (tag.len() == DOCUMENT_TAG_BYTES)
        .then(|| MultibaseKey::try_from(key).ok())
        .flatten()
}

fn describe(error: &ClaimHistoryError) -> String {
    match error {
        ClaimHistoryError::ResourceRefused(reason) => format!("resource limit: {reason}"),
        ClaimHistoryError::Unsupported(reason) => format!("unsupported: {reason}"),
        ClaimHistoryError::MalformedWitness(reason) => format!("malformed: {reason}"),
        ClaimHistoryError::InvalidSignature => "invalid signature".into(),
        ClaimHistoryError::Automerge(reason) => format!("Automerge: {reason}"),
    }
}

fn unreadable(error: ClaimHistoryError) -> Verdict {
    match error {
        ClaimHistoryError::MalformedWitness(_)
        | ClaimHistoryError::InvalidSignature
        | ClaimHistoryError::Automerge(_) => Verdict::Rejected {
            reason: describe(&error),
        },
        ClaimHistoryError::ResourceRefused(_) | ClaimHistoryError::Unsupported(_) => {
            Verdict::Pending {
                reason: describe(&error),
                facet_heads: Vec::new(),
            }
        }
    }
}

/// The introductions of the signature entries on `key` visible at `heads`, without verifying any of
/// them. This is what a listing reports; reads report the verified subset.
///
/// Read failures are swallowed to "nothing here": a listing is deliberately unverified, so an
/// unreadable container is reported as an absent slot rather than turned into a verification error.
pub(super) fn signature_heads(
    doc: &Automerge,
    key: &FacetKey,
    heads: &[ChangeHash],
) -> Vec<ChangeHash> {
    let maps = |object: &automerge::ObjId, prop: &str| -> Vec<automerge::ObjId> {
        doc.get_all_at(object, prop, heads)
            .unwrap_or_default()
            .into_iter()
            .filter(|(value, _)| matches!(value, Value::Object(ObjType::Map)))
            .map(|(_, id)| id)
            .collect()
    };
    let key = key.to_string();
    let mut found = BTreeSet::new();
    for facets in maps(&automerge::ROOT, "facets") {
        for dmeta in maps(&facets, DMETA_KEY) {
            for metadata in maps(&dmeta, "facets") {
                for facet in maps(&metadata, &key) {
                    for (value, list) in doc
                        .get_all_at(&facet, "signatures", heads)
                        .unwrap_or_default()
                    {
                        if value != Value::Object(ObjType::List) {
                            continue;
                        }
                        for index in 0..doc.length_at(&list, heads) {
                            for (_, marker) in
                                doc.get_all_at(&list, index, heads).unwrap_or_default()
                            {
                                found.extend(doc.hash_for_opid(&marker));
                            }
                        }
                    }
                }
            }
        }
    }
    found.into_iter().collect()
}
