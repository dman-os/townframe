//! The read and write gate: is datum X at heads H exactly what valid signatures say it is?
//!
//! Every change that modifies a datum's content also introduces a signature on that datum, and the
//! signature covers the datum's value as that change left it. The datum's **datum heads** are the
//! introductions of the signatures currently covering it. X at H is accepted when:
//!
//! 1. **The current signatures verify.** Each claim, reconstructed at its introduction, matches the
//!    signed bytes, and the actor's attribution certificate verifies against the node that made
//!    the change. A proven-invalid current entry rejects that datum.
//! 2. **Nothing touched X outside them.** Every change in H's history that touched X and is not
//!    already beneath the datum heads must itself introduce a valid signature on X. A change that
//!    touched X without signing it — even one whose value lost an Automerge conflict and is not
//!    visible at H — is a forgery and rejects the datum.
//! 3. **The value is what they signed.** The value at H equals the value at the datum heads, i.e.
//!    the signed values merged by Automerge's own rules. This stops signature entries from being
//!    deleted out from under a value.
//!
//! Changes beneath the datum heads are not re-examined: whoever signed on top of them passed this
//! same gate for their base first. That is why a hidden unsigned contribution cannot be laundered.
//! An honest writer's base check sees it (it is not beneath the signatures that existed then) and
//! refuses to sign; only a writer bypassing the gate could sign over it, and their signature is
//! then theirs to answer for. It is also what keeps a slot recoverable: an explicit repair signs a
//! new value and the old forgery falls beneath it.
//!
//! Pending means verification could not complete; it never means part of an applied change may
//! arrive later. Missing document metadata or attribution certificates are malformed, not pending.
//! Checks are datum-local: a forgery in another datum changes nothing here.

use super::claim_history::ClaimHistoryError;
use super::claim_history::ClaimHistoryResult;
use super::claim_history::HistoryBudget;
use super::claim_history::ReconstructedClaim;
use super::claim_history::reconstruct_claim;
use super::claim_history::reconstruct_current_claims;
use automerge::ActorId;
use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::Value;
use automerge::legacy::Key;
use automerge::legacy::ObjectId;
use automerge::legacy::OpId;
use automerge::legacy::OpType;
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
use std::cell::OnceCell;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

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

#[derive(Default)]
struct Footprint {
    touched: Vec<usize>,
    signed: HashSet<usize>,
}

struct Footprints {
    hashes: Vec<ChangeHash>,
    index_of: HashMap<ChangeHash, usize>,
    deps: Vec<Vec<usize>>,
    by_slot: HashMap<String, Footprint>,
    envelope_changes: Vec<usize>,
}

pub(crate) struct Gate<'a> {
    doc: &'a Automerge,
    heads: &'a [ChangeHash],
    footprints: OnceCell<Footprints>,
    budget: HistoryBudget,
}

enum Role {
    Datums,
    Slot(String),
    Dmeta,
    DmetaDatums,
    DatumMeta(String),
    Signatures(String),
}

impl<'a> Gate<'a> {
    pub(crate) fn new(doc: &'a Automerge, heads: &'a [ChangeHash]) -> Self {
        Self {
            doc,
            heads,
            footprints: OnceCell::new(),
            budget: HistoryBudget::default(),
        }
    }

    /// One pass over H's history records, per datum key, which changes touched the datum's
    /// content and which introduced a signature on it. Computed lazily: a datum whose signature
    /// heads already cover `self.heads` has no uncovered history to inspect.
    fn footprints(&self) -> &Footprints {
        self.footprints.get_or_init(|| {
            let ancestry = ancestry(self.doc, self.heads.iter().copied());
            let mut hashes = Vec::new();
            let mut index_of = HashMap::new();
            let mut deps = Vec::new();
            let mut roles: HashMap<OpId, Role> = HashMap::new();
            let mut by_slot: HashMap<String, Footprint> = HashMap::new();
            let mut envelope_changes = Vec::new();
            for change in self.doc.get_changes(&[]) {
                let hash = change.hash();
                if !ancestry.contains(&hash) {
                    continue;
                }
                let change_idx = hashes.len();
                hashes.push(hash);
                index_of.insert(hash, change_idx);
                deps.push(
                    change
                        .deps()
                        .iter()
                        .filter_map(|dep| index_of.get(dep).copied())
                        .collect(),
                );
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
                        (ObjectId::Root, Some("datums")) => true,
                        (ObjectId::Id(parent), Some(key)) => matches!(
                            (roles.get(parent), key),
                            (Some(Role::Datums), DMETA_KEY) | (Some(Role::Dmeta), "datums")
                        ),
                        _ => false,
                    };
                    if shared && envelope_changes.last() != Some(&change_idx) {
                        envelope_changes.push(change_idx);
                    }
                    let role = match &op.obj {
                        ObjectId::Root => match (key, &op.action) {
                            (Some("datums"), OpType::Make(ObjType::Map)) => Some(Role::Datums),
                            _ => None,
                        },
                        ObjectId::Id(parent) => match roles.get(parent) {
                            None => None,
                            Some(Role::Datums) => match key {
                                Some(DMETA_KEY) => matches!(op.action, OpType::Make(ObjType::Map))
                                    .then_some(Role::Dmeta),
                                Some(slot) => {
                                    touch(&mut by_slot, slot, change_idx);
                                    made.then(|| Role::Slot(slot.to_owned()))
                                }
                                None => None,
                            },
                            Some(Role::Slot(slot)) => {
                                let slot = slot.clone();
                                touch(&mut by_slot, &slot, change_idx);
                                made.then_some(Role::Slot(slot))
                            }
                            Some(Role::Dmeta) => (key == Some("datums")
                                && matches!(op.action, OpType::Make(ObjType::Map)))
                            .then_some(Role::DmetaDatums),
                            Some(Role::DmetaDatums) => match key {
                                Some(slot) => {
                                    touch(&mut by_slot, slot, change_idx);
                                    matches!(op.action, OpType::Make(ObjType::Map))
                                        .then(|| Role::DatumMeta(slot.to_owned()))
                                }
                                None => None,
                            },
                            Some(Role::DatumMeta(slot)) => {
                                if key == Some("signatures") {
                                    touch(&mut by_slot, slot, change_idx);
                                    matches!(op.action, OpType::Make(ObjType::List))
                                        .then(|| Role::Signatures(slot.clone()))
                                } else {
                                    None
                                }
                            }
                            Some(Role::Signatures(slot)) => {
                                touch(&mut by_slot, slot, change_idx);
                                if op.insert && matches!(op.action, OpType::Make(ObjType::Map)) {
                                    by_slot
                                        .entry(slot.clone())
                                        .or_default()
                                        .signed
                                        .insert(change_idx);
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
            Footprints {
                hashes,
                index_of,
                deps,
                by_slot,
                envelope_changes,
            }
        })
    }

    /// Every datum key touched in `heads`'s history, including slots later deleted.
    pub(super) fn keys(&self) -> impl Iterator<Item = DatumKey> + '_ {
        self.footprints()
            .by_slot
            .keys()
            .filter_map(|key| key.parse().ok())
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
        let heads_all_covered = self.heads.iter().all(|head| datum_heads.contains(head));
        if !heads_all_covered {
            let footprints = self.footprints();
            if let Some(footprint) = footprints.by_slot.get(&key.to_string()) {
                let mut covered = vec![false; footprints.hashes.len()];
                let mut stack: Vec<usize> = datum_heads
                    .iter()
                    .filter_map(|head| footprints.index_of.get(head).copied())
                    .collect();
                while let Some(idx) = stack.pop() {
                    if !covered[idx] {
                        covered[idx] = true;
                        stack.extend(footprints.deps[idx].iter().copied());
                    }
                }
                for &change_idx in footprint
                    .touched
                    .iter()
                    .chain(&footprints.envelope_changes)
                    .filter(|&&change_idx| !covered[change_idx])
                {
                    let change = footprints.hashes[change_idx];
                    if !footprint.signed.contains(&change_idx) {
                        return Verdict::Rejected {
                            reason: format!(
                                "change {change} modified the datum without signing it"
                            ),
                        };
                    }
                    match self.claim_status(reconstruct_claim(self.doc, key, change, self.budget)) {
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
        }
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

fn touch(footprints: &mut HashMap<String, Footprint>, slot: &str, change_idx: usize) {
    let touched = &mut footprints.entry(slot.to_owned()).or_default().touched;
    if touched.last() != Some(&change_idx) {
        touched.push(change_idx);
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
pub(super) fn datum_version(doc: &Automerge, heads: Vec<ChangeHash>) -> DatumVersion {
    let clock = heads
        .iter()
        .filter_map(|head| doc.get_change_by_hash(head))
        .map(|change| change.max_op())
        .max()
        .unwrap_or(0);
    DatumVersion { heads, clock }
}

/// The introductions of the signature entries on `key` visible at `heads`, without verifying any of
/// them. This is what a listing reports; reads report the verified subset.
///
/// Read failures are swallowed to "nothing here": a listing is deliberately unverified, so an
/// unreadable container is reported as an absent slot rather than turned into a verification error.
pub(super) fn signature_heads(
    doc: &Automerge,
    key: &DatumKey,
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
    for datums in maps(&automerge::ROOT, "datums") {
        for dmeta in maps(&datums, DMETA_KEY) {
            for metadata in maps(&dmeta, "datums") {
                for datum in maps(&metadata, &key) {
                    for (value, list) in doc
                        .get_all_at(&datum, "signatures", heads)
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
