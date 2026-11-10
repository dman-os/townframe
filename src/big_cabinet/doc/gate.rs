//! The read and write gate: is datum X at heads H exactly what valid signatures say it is?
//!
//! Every change that modifies a datum's content also introduces a signature on that datum, and the
//! signature covers the datum's value as that change left it. The datum's **datum heads** are the
//! introductions of the signatures currently covering it. X at H is accepted when:
//!
//! 1. **The current signatures verify.** Each claim, reconstructed at its introduction, matches the
//!    signed bytes, and the actor's attribution certificate verifies against the node that made
//!    the change. A proven-invalid current entry rejects that datum.
//! 2. **The value is what they signed.** The value at H equals the value at the datum heads, i.e.
//!    the signed values merged by Automerge's own rules. An unsigned change that is visible at H
//!    makes the two differ, and deleting signature entries out from under a value shrinks the
//!    datum heads until they no longer produce it. A slot with no signatures has no datum heads,
//!    so it is accepted only when it is absent at H.
//!
//! The gate reads the datum at two sets of heads, never the document's history. It does not look
//! for unsigned operations that lost an Automerge conflict and are invisible at H: the docs service
//! offers no read of conflicted values, so such an operation cannot reach a reader or a writer's
//! base. If a read of conflicted values is ever added, forgery checking belongs on that path.
//!
//! Pending means verification could not complete; it never means part of an applied change may
//! arrive later. Missing document metadata or attribution certificates are malformed, not pending.
//! Checks are datum-local: a forgery in another datum changes nothing here.

use super::claim_history::ClaimHistoryError;
use super::claim_history::ClaimHistoryResult;
use super::claim_history::HistoryBudget;
use super::claim_history::ReconstructedClaim;
use super::claim_history::reconstruct_current_claims;
use automerge::ActorId;
use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::Value;
use big_cabinet_core::DatumKey;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::dmeta::ActorMetadata;
use big_cabinet_core::dmeta::Attribution;
use big_cabinet_core::dmeta::AttributionError;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::Dmeta;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::NativeReadBudget;
use big_cabinet_core::dmeta::NativeValue;
use big_cabinet_core::dmeta::cid_of;
use big_cabinet_core::dmeta::encode_native;
use big_cabinet_core::doc::DatumVersion;
use std::collections::BTreeSet;

pub(super) const DMETA_KEY: &str = "org.example.daybook.dmeta/main";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    Accepted {
        datum_heads: Vec<ChangeHash>,
        native: Option<Vec<u8>>,
    },
    Pending {
        reason: String,
        datum_heads: Vec<ChangeHash>,
    },
    Rejected {
        reason: String,
    },
}

pub(crate) struct Gate<'a> {
    doc: &'a Automerge,
    heads: &'a [ChangeHash],
    budget: HistoryBudget,
}

impl<'a> Gate<'a> {
    pub(crate) fn new(doc: &'a Automerge, heads: &'a [ChangeHash]) -> Self {
        Self {
            doc,
            heads,
            budget: HistoryBudget::default(),
        }
    }

    /// Every datum key the gate has a verdict to give on at `heads`: slots with content, and slots
    /// whose metadata is still recorded (a signed removal leaves no content but keeps its
    /// signatures). A key in neither has no signatures and no content, which is always accepted
    /// as absent, so it needs no check. Conflicting envelopes are all read, as
    /// [`signature_heads`] does, because the gate reads every signature on a slot.
    pub(super) fn keys(&self) -> BTreeSet<DatumKey> {
        let maps = |object: &automerge::ObjId, prop: &str| -> Vec<automerge::ObjId> {
            self.doc
                .get_all_at(object, prop, self.heads)
                .expect("the gate runs on heads the caller checked are known")
                .into_iter()
                .filter(|(value, _)| matches!(value, Value::Object(ObjType::Map)))
                .map(|(_, id)| id)
                .collect()
        };
        let keys = |object: &automerge::ObjId| {
            self.doc
                .map_range_at(object, .., self.heads)
                .filter(|item| item.key != DMETA_KEY)
                .filter_map(|item| item.key.parse::<DatumKey>().ok())
                .collect::<Vec<_>>()
        };
        let mut found = BTreeSet::new();
        for datums in maps(&automerge::ROOT, "datums") {
            found.extend(keys(&datums));
            for dmeta in maps(&datums, DMETA_KEY) {
                for metadata in maps(&dmeta, "datums") {
                    found.extend(keys(&metadata));
                }
            }
        }
        found
    }

    pub(crate) fn verify(&self, key: &DatumKey) -> Verdict {
        assert_ne!(key.tag().as_str(), Dmeta::ID, "dmeta is not a signed datum");
        let mut pending: Option<String> = None;
        let entries = match reconstruct_current_claims(self.doc, key, self.heads, self.budget) {
            Ok(entries) => entries,
            Err(error) => return unreadable(error),
        };
        let mut datum_heads = BTreeSet::new();
        let mut single_claim_native: Option<(ChangeHash, Option<Vec<u8>>)> = None;
        let entries_len = entries.len();
        for entry in entries {
            let introduction = self
                .doc
                .hash_for_opid(&entry.marker)
                .expect("an observed operation belongs to an applied change");
            if entries_len == 1
                && let ClaimHistoryResult::Accepted(claim) = &entry.result
                && let Some(native) = &claim.native
            {
                single_claim_native = Some((introduction, native.clone()));
            }
            match self.claim_status(entry.result) {
                Status::Valid => {
                    datum_heads.insert(introduction);
                }
                Status::Pending(reason) => {
                    datum_heads.insert(introduction);
                    pending.get_or_insert(reason);
                }
                Status::Invalid(reason) => return Verdict::Rejected { reason },
            }
        }
        // The signed value is the datum read at the datum heads: Automerge merges concurrent
        // signed values there by its own rules. Anything that changed the visible value without
        // signing it — an unsigned edit, or a signature entry deleted out from under its value —
        // leaves the value at `self.heads` different from it.
        let datum_heads: Vec<_> = datum_heads.into_iter().collect();
        let signed = if datum_heads.is_empty() {
            None
        } else if let Some((intro, native)) = single_claim_native
            && datum_heads.as_slice() == [intro]
        {
            native
        } else {
            match self.native(key, &datum_heads) {
                Ok(native) => native,
                Err(error) => return unreadable(error),
            }
        };
        let current = if self.heads == datum_heads.as_slice() {
            signed.clone()
        } else {
            match self.native(key, self.heads) {
                Ok(native) => native,
                Err(error) => return unreadable(error),
            }
        };
        if current != signed {
            return Verdict::Rejected {
                reason: "the value is not the one its signatures cover".into(),
            };
        };
        match pending {
            Some(reason) => Verdict::Pending {
                reason,
                datum_heads,
            },
            None => Verdict::Accepted {
                datum_heads,
                native: current,
            },
        }
    }

    fn native(
        &self,
        key: &DatumKey,
        heads: &[ChangeHash],
    ) -> Result<Option<Vec<u8>>, ClaimHistoryError> {
        let Some((value, datums)) = self.doc.get_at(automerge::ROOT, "datums", heads)? else {
            return Ok(None);
        };
        if value != Value::Object(ObjType::Map) {
            return Err(ClaimHistoryError::MalformedWitness("datums map"));
        }
        let mut budget = NativeReadBudget {
            nodes: self.budget.max_value_nodes,
            bytes: self.budget.max_bytes,
            max_depth: self.budget.max_depth,
        };
        Ok(
            NativeValue::read_at_heads(self.doc, &datums, key.to_string(), heads, &mut budget)?
                .map(|native| encode_native(&native)),
        )
    }

    fn claim_status(&self, result: ClaimHistoryResult) -> Status {
        match result {
            ClaimHistoryResult::Accepted(claim) => {
                if claim.signature.inherited.is_some() {
                    let native = match self
                        .native(&claim.claim.datum_key, &claim.introduction.resulting_heads)
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
        extra: Default::default(),
        created_at: Vec::new(),
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

/// The Automerge actor DocsRepo authors a document's changes under: the node key, then the
/// first 16 bytes of the BLAKE3 hash of the document id.
///
/// The node key is what a verifier reads back to find the expected issuer of the attribution
/// certificates a change carries. The document tag is there because Automerge identifies a change
/// by its actor and sequence number: a branch starts as its origin's changes, and if both were then
/// written under the bare node key, each would hold a different change at the same sequence number
/// and merging the branch back would be refused.
pub(super) fn doc_actor(node: &MultibaseKey, document: &DocId) -> ActorId {
    let mut bytes = node.as_bytes().to_vec();
    bytes.extend_from_slice(&blake3::hash(document.as_bytes()).as_bytes()[..DOCUMENT_TAG_BYTES]);
    ActorId::from(bytes)
}

/// The node key a change was authored under ([`doc_actor`]). A write at historical heads makes
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
                datum_heads: Vec::new(),
            }
        }
    }
}

/// The version a set of datum heads names: those heads plus their Lamport
/// clock (the greatest `max_op` among the introducing changes).
///
/// The clock comes from change metadata, not `get_change_by_hash`: the latter re-encodes the whole
/// change by scanning the document's operations, which made a listing quadratic in its slots.
pub(super) fn datum_version(doc: &Automerge, heads: Vec<ChangeHash>) -> DatumVersion {
    let clock = heads
        .iter()
        .filter_map(|head| doc.get_change_meta_by_hash(head))
        .map(|change| change.max_op)
        .max()
        .unwrap_or(0);
    DatumVersion { heads, clock }
}

/// The introductions of the signature entries on `key` in `view`, without verifying any of them.
/// This is what a listing reports; reads report the verified subset.
///
/// `view` must already be the document at the heads being listed: the caller passes the live
/// document for current heads and a single `fork_at` for historical ones. Each `*_at` read in
/// Automerge rebuilds a clock from the explicit heads, costing time proportional to the document,
/// so calling them per slot made a listing quadratic in the slot count.
///
/// Read failures are swallowed to "nothing here": a listing is deliberately unverified, so an
/// unreadable container is reported as an absent slot rather than turned into a verification error.
pub(super) fn signature_heads(view: &Automerge, key: &DatumKey) -> Vec<ChangeHash> {
    let maps = |object: &automerge::ObjId, prop: &str| -> Vec<automerge::ObjId> {
        view.get_all(object, prop)
            .unwrap_or_default()
            .into_iter()
            .filter(|(value, _)| matches!(value, Value::Object(ObjType::Map)))
            .map(|(_, id)| id)
            .collect()
    };
    let key = key.to_string();
    let mut found = BTreeSet::new();
    for datums in maps(&automerge::ROOT, "datums") {
        for dmeta in maps(&datums, DMETA_KEY) {
            for metadata in maps(&dmeta, "datums") {
                for datum in maps(&metadata, &key) {
                    for (value, list) in view.get_all(&datum, "signatures").unwrap_or_default() {
                        if value != Value::Object(ObjType::List) {
                            continue;
                        }
                        for index in 0..view.length(&list) {
                            for (_, marker) in view.get_all(&list, index).unwrap_or_default() {
                                found.extend(view.hash_for_opid(&marker));
                            }
                        }
                    }
                }
            }
        }
    }
    found.into_iter().collect()
}
