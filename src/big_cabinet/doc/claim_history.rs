use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjId;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::Value;
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::Bytes;
use big_cabinet_core::dmeta::Cid;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::FacetClaim;
use big_cabinet_core::dmeta::FacetSignature;
use big_cabinet_core::dmeta::InheritedWitness;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::NativeReadBudget;
use big_cabinet_core::dmeta::NativeReadError;
use big_cabinet_core::dmeta::NativeValue;
use big_cabinet_core::dmeta::SignatureBytes;
use big_cabinet_core::dmeta::SignedFacetClaim;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::dmeta::cid_of;
use big_cabinet_core::dmeta::encode_native;
use big_cabinet_core::dmeta::signature_id;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub struct HistoryBudget {
    pub max_claims: usize,
    pub max_entries_per_frontier: usize,
    pub max_value_nodes: usize,
    pub max_bytes: usize,
    pub max_depth: usize,
}

impl Default for HistoryBudget {
    fn default() -> Self {
        Self {
            max_claims: 256,
            max_entries_per_frontier: 1024,
            max_value_nodes: 16_384,
            max_bytes: 16 * 1024 * 1024,
            max_depth: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimHistoryError {
    ResourceRefused(&'static str),
    Unsupported(&'static str),
    MalformedWitness(&'static str),
    InvalidSignature,
    Automerge(String),
}

impl ClaimHistoryError {
    fn pending(&self) -> bool {
        matches!(self, Self::ResourceRefused(_) | Self::Unsupported(_))
    }
}

impl From<automerge::AutomergeError> for ClaimHistoryError {
    fn from(error: automerge::AutomergeError) -> Self {
        Self::Automerge(error.to_string())
    }
}

impl From<NativeReadError> for ClaimHistoryError {
    fn from(error: NativeReadError) -> Self {
        match error {
            NativeReadError::Automerge(error) => error.into(),
            NativeReadError::Unsupported(kind) => Self::Unsupported(kind),
            NativeReadError::ResourceLimit(limit) => Self::ResourceRefused(limit),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimHistoryResult {
    Accepted(Box<ReconstructedClaim>),
    Pending(ClaimHistoryError),
    ProvenForgery(ClaimHistoryError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimHistoryEntry {
    pub marker: ObjId,
    pub result: ClaimHistoryResult,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignatureIntroduction {
    pub marker: ObjId,
    pub introduction_hash: ChangeHash,
    pub parents: Vec<ChangeHash>,
    pub resulting_heads: Vec<ChangeHash>,
    pub automerge_actor: automerge::ActorId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReconstructedClaim {
    pub introduction: SignatureIntroduction,
    pub signature: FacetSignature,
    pub actor_public_key: MultibaseKey,
    /// The binding's actor URL and attribution certificate as they stood at the introduction.
    pub actor_url: Option<String>,
    pub certificate: Option<Bytes>,
    pub claim: FacetClaim,
    /// The canonical native bytes read at `introduction.resulting_heads` for a non-inherited claim.
    pub(crate) native: Option<Option<Vec<u8>>>,
}

/// The sorted signature IDs a claim written at `heads` must name as predecessors: one per
/// signature entry visible there, whether or not that entry verifies. This is the same derivation
/// reconstruction uses at an introduction's parents, so a writer and a verifier cannot disagree.
pub fn predecessor_ids(
    doc: &Automerge,
    facet_key: &FacetKey,
    heads: &[ChangeHash],
    budget: HistoryBudget,
) -> Result<Vec<Cid>, ClaimHistoryError> {
    Reconstruction::new(doc, facet_key, budget).predecessor_ids(heads)
}

pub fn reconstruct_claim_for_marker(
    doc: &Automerge,
    facet_key: &FacetKey,
    marker: &ObjId,
    budget: HistoryBudget,
) -> ClaimHistoryResult {
    let mut state = Reconstruction::new(doc, facet_key, budget);
    let result = doc
        .hash_for_opid(marker)
        .ok_or(ClaimHistoryError::MalformedWitness(
            "signature introduction change",
        ))
        .and_then(|hash| state.claim_for_entry(hash, marker.clone()));
    state.verdict(result)
}

pub fn reconstruct_claim(
    doc: &Automerge,
    facet_key: &FacetKey,
    introduction_hash: ChangeHash,
    budget: HistoryBudget,
) -> ClaimHistoryResult {
    let mut state = Reconstruction::new(doc, facet_key, budget);
    let result = (|| {
        state.check_heads(&[introduction_hash])?;
        let entries: Vec<_> = state
            .entries(&[introduction_hash])?
            .into_iter()
            .filter(|entry| entry.hash == introduction_hash)
            .collect();
        let [entry] = entries.as_slice() else {
            return Err(ClaimHistoryError::MalformedWitness(
                "one signature marker at introduction",
            ));
        };
        state.claim_for_entry(introduction_hash, entry.marker.clone())
    })();
    state.verdict(result)
}

pub fn reconstruct_current_claims(
    doc: &Automerge,
    facet_key: &FacetKey,
    heads: &[ChangeHash],
    budget: HistoryBudget,
) -> Result<Vec<ClaimHistoryEntry>, ClaimHistoryError> {
    let mut state = Reconstruction::new(doc, facet_key, budget);
    state.check_heads(heads)?;
    let entries = state.entries(heads)?;
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        let result = (|| {
            let hash = entry.hash;
            if !entry.is_map {
                return Err(ClaimHistoryError::MalformedWitness(
                    "signature entry is not a map",
                ));
            }
            let claim = state.claim_for_entry(hash, entry.marker.clone())?;
            if state.read_signature(&entry.marker, heads)? != claim.signature {
                return Err(ClaimHistoryError::MalformedWitness(
                    "signature entry changed after introduction",
                ));
            }
            Ok(claim)
        })();
        results.push(ClaimHistoryEntry {
            marker: entry.marker,
            result: state.verdict(result),
        });
    }
    Ok(results)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SignatureKey {
    hash: ChangeHash,
    marker: ObjId,
}

#[derive(Clone)]
struct SignatureEntry {
    marker: ObjId,
    hash: ChangeHash,
    is_map: bool,
}

struct Reconstruction<'a> {
    doc: &'a Automerge,
    facet_key: &'a FacetKey,
    budget: HistoryBudget,
    native_budget: NativeReadBudget,
    claims_seen: usize,
    cache: HashMap<SignatureKey, ReconstructedClaim>,
    frontiers: HashMap<Vec<ChangeHash>, Vec<SignatureEntry>>,
    frontier_reads: HashMap<Vec<ChangeHash>, usize>,
    active: HashSet<SignatureKey>,
}

impl<'a> Reconstruction<'a> {
    fn new(doc: &'a Automerge, facet_key: &'a FacetKey, budget: HistoryBudget) -> Self {
        Self {
            doc,
            facet_key,
            budget,
            native_budget: NativeReadBudget {
                nodes: budget.max_value_nodes,
                bytes: budget.max_bytes,
                max_depth: budget.max_depth,
            },
            claims_seen: 0,
            cache: HashMap::new(),
            frontiers: HashMap::new(),
            frontier_reads: HashMap::new(),
            active: HashSet::new(),
        }
    }

    fn verdict(&self, result: Result<ReconstructedClaim, ClaimHistoryError>) -> ClaimHistoryResult {
        let result = result.and_then(|claim| {
            SignedFacetClaim {
                claim: claim.claim.clone(),
                actor_public_key: claim.actor_public_key.clone(),
                signature: claim.signature.signature.clone(),
            }
            .verify()
            .map_err(|_| ClaimHistoryError::InvalidSignature)?;
            Ok(claim)
        });
        match result {
            Ok(claim) => ClaimHistoryResult::Accepted(Box::new(claim)),
            Err(error) if error.pending() => ClaimHistoryResult::Pending(error),
            Err(error) => ClaimHistoryResult::ProvenForgery(error),
        }
    }

    fn check_heads(&self, heads: &[ChangeHash]) -> Result<(), ClaimHistoryError> {
        if heads.len() > self.budget.max_entries_per_frontier {
            return Err(ClaimHistoryError::ResourceRefused("heads frontier"));
        }
        if heads
            .iter()
            .any(|hash| self.doc.get_change_by_hash(hash).is_none())
        {
            return Err(ClaimHistoryError::MalformedWitness("unknown heads"));
        }
        Ok(())
    }

    fn account(&mut self, heads: &[ChangeHash], count: usize) -> Result<(), ClaimHistoryError> {
        let mut key = heads.to_vec();
        key.sort();
        key.dedup();
        let used = self.frontier_reads.entry(key).or_default();
        *used = used
            .checked_add(count)
            .ok_or(ClaimHistoryError::ResourceRefused("metadata frontier"))?;
        if *used > self.budget.max_entries_per_frontier {
            return Err(ClaimHistoryError::ResourceRefused("metadata frontier"));
        }
        Ok(())
    }
}

impl Reconstruction<'_> {
    fn claim_for_entry(
        &mut self,
        hash: ChangeHash,
        marker: ObjId,
    ) -> Result<ReconstructedClaim, ClaimHistoryError> {
        let key = SignatureKey {
            hash,
            marker: marker.clone(),
        };
        if let Some(claim) = self.cache.get(&key) {
            return Ok(claim.clone());
        }
        if self.claims_seen >= self.budget.max_claims || self.active.len() >= self.budget.max_depth
        {
            return Err(ClaimHistoryError::ResourceRefused("claim history"));
        }
        if !self.active.insert(key.clone()) {
            return Err(ClaimHistoryError::MalformedWitness(
                "cyclic signature ancestry",
            ));
        }
        self.claims_seen += 1;
        let result = self.claim_uncached(hash, marker);
        self.active.remove(&key);
        if let Ok(claim) = &result {
            self.cache.insert(key, claim.clone());
        }
        result
    }

    fn claim_uncached(
        &mut self,
        hash: ChangeHash,
        marker: ObjId,
    ) -> Result<ReconstructedClaim, ClaimHistoryError> {
        let change = self
            .doc
            .get_change_by_hash(&hash)
            .expect("an observed operation belongs to an applied change");
        let parents = change.deps().to_vec();
        let automerge_actor = change.actor_id().clone();
        let resulting_heads = vec![hash];
        let entry = self
            .entries(&resulting_heads)?
            .into_iter()
            .find(|entry| entry.marker == marker)
            .ok_or(ClaimHistoryError::MalformedWitness(
                "marker outside target signature set",
            ))?;
        if !entry.is_map || entry.hash != hash {
            return Err(ClaimHistoryError::MalformedWitness(
                "signature marker introduction",
            ));
        }
        let signature = self.read_signature(&marker, &resulting_heads)?;
        let (actor_public_key, actor_url, certificate) =
            self.actor_binding(signature.actor_uuid, &resulting_heads)?;
        let aliases = self.aliases(&resulting_heads)?;
        let (claim, native) = if let Some(witness) = &signature.inherited {
            if signature.message.is_some() {
                return Err(ClaimHistoryError::MalformedWitness(
                    "multiple message carriers",
                ));
            }
            let claim = FacetClaim::from_signing_bytes(witness.message.as_slice())
                .map_err(|_| ClaimHistoryError::MalformedWitness("inherited message"))?;
            if claim.origin_document != witness.origin_doc
                || witness.value.as_ref().is_some_and(|value| {
                    claim.operation != ClaimOperation::Set
                        || claim.value_cid.as_ref() != Some(&cid_of(value.as_slice()))
                })
            {
                return Err(ClaimHistoryError::MalformedWitness("inherited context"));
            }
            (claim, None)
        } else {
            let encoded = self
                .facet_value(&resulting_heads)?
                .map(|value| encode_native(&value));
            let claim = FacetClaim {
                // A signature reaches other documents unchanged through branches and merges, so
                // the document its claim names is read from the binding of its `docUuid` at the
                // introduction, never assumed to be the document being read.
                origin_document: self.document(signature.doc_uuid, &resulting_heads)?,
                facet_key: self.facet_key.clone(),
                chosen_uuid: *aliases
                    .first()
                    .ok_or(ClaimHistoryError::MalformedWitness("facet UUID alias"))?,
                actor_uuid: signature.actor_uuid,
                operation: if encoded.is_some() {
                    ClaimOperation::Set
                } else {
                    ClaimOperation::Remove
                },
                value_cid: encoded.as_deref().map(cid_of),
                predecessors: self.predecessor_ids(&parents)?,
            };
            if let Some(message) = &signature.message
                && claim
                    .signing_bytes()
                    .map_err(|_| ClaimHistoryError::MalformedWitness("claim context"))?
                    .as_slice()
                    != message.as_slice()
            {
                return Err(ClaimHistoryError::MalformedWitness(
                    "carried and reconstructed messages disagree",
                ));
            }
            (claim, Some(encoded))
        };
        if claim.actor_uuid != signature.actor_uuid
            || claim.facet_key != *self.facet_key
            || !aliases.contains(&claim.chosen_uuid)
        {
            return Err(ClaimHistoryError::MalformedWitness("claim binding"));
        }
        Ok(ReconstructedClaim {
            introduction: SignatureIntroduction {
                marker,
                introduction_hash: hash,
                parents,
                resulting_heads,
                automerge_actor,
            },
            signature,
            actor_public_key,
            actor_url,
            certificate,
            claim,
            native,
        })
    }

    fn predecessor_ids(&mut self, heads: &[ChangeHash]) -> Result<Vec<Cid>, ClaimHistoryError> {
        let mut ids = Vec::new();
        for entry in self.entries(heads)? {
            if !entry.is_map {
                continue;
            }
            let resulting_heads = [entry.hash];
            // A predecessor's ID depends only on the actor's public key and the signature bytes
            // recorded at its introduction, never on reconstructing or verifying the parent claim
            // or its own ancestors. A structurally malformed entry is skipped deterministically;
            // a resource refusal still surfaces as pending.
            let introduced = match self.entries(&resulting_heads) {
                Ok(entries) => entries.into_iter().find(|item| item.marker == entry.marker),
                Err(error) if !error.pending() => continue,
                Err(error) => return Err(error),
            };
            if !introduced.is_some_and(|item| item.is_map && item.hash == entry.hash) {
                continue;
            }
            let signature = match self.read_signature(&entry.marker, &resulting_heads) {
                Ok(signature) => signature,
                Err(error) if !error.pending() => continue,
                Err(error) => return Err(error),
            };
            let (actor_public_key, _, _) =
                match self.actor_binding(signature.actor_uuid, &resulting_heads) {
                    Ok(binding) => binding,
                    Err(error) if !error.pending() => continue,
                    Err(error) => return Err(error),
                };
            ids.push(signature_id(&actor_public_key, &signature.signature));
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn entries(&mut self, heads: &[ChangeHash]) -> Result<Vec<SignatureEntry>, ClaimHistoryError> {
        self.check_heads(heads)?;
        let mut frontier = heads.to_vec();
        frontier.sort();
        frontier.dedup();
        if let Some(entries) = self.frontiers.get(&frontier) {
            return Ok(entries.clone());
        }
        let mut lists = Vec::new();
        for meta in self.facet_metas(heads)? {
            let values = self.doc.get_all_at(&meta, "signatures", heads)?;
            self.account(heads, values.len())?;
            for (value, list) in values {
                if value != Value::Object(ObjType::List) {
                    return Err(ClaimHistoryError::MalformedWitness(
                        "signatures is not a list",
                    ));
                }
                if !lists.contains(&list) {
                    lists.push(list);
                }
            }
        }
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        for list in lists {
            let length = self.doc.length_at(&list, heads);
            if length > self.budget.max_entries_per_frontier {
                return Err(ClaimHistoryError::ResourceRefused("signature list"));
            }
            for index in 0..length {
                let values = self.doc.get_all_at(&list, index, heads)?;
                self.account(heads, values.len())?;
                for (value, marker) in values {
                    if seen.insert(marker.clone()) {
                        entries.push(SignatureEntry {
                            hash: self
                                .doc
                                .hash_for_opid(&marker)
                                .expect("an observed operation belongs to an applied change"),
                            marker,
                            is_map: value == Value::Object(ObjType::Map),
                        });
                    }
                }
            }
        }
        if self.frontiers.len() <= self.budget.max_claims {
            self.frontiers.insert(frontier, entries.clone());
        }
        Ok(entries)
    }

    fn maps(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
    ) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let values = self.doc.get_all_at(object, property, heads)?;
        self.account(heads, values.len())?;
        let mut objects = Vec::new();
        for (value, id) in values {
            if value != Value::Object(ObjType::Map) {
                return Err(ClaimHistoryError::MalformedWitness("metadata map"));
            }
            if !objects.contains(&id) {
                objects.push(id);
            }
        }
        Ok(objects)
    }

    fn dmeta_maps(&mut self, heads: &[ChangeHash]) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let mut result = Vec::new();
        for facets in self.maps(&automerge::ROOT, "facets", heads)? {
            result.extend(self.maps(&facets, "org.example.daybook.dmeta/main", heads)?);
        }
        Ok(result)
    }

    fn facet_metas(&mut self, heads: &[ChangeHash]) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let mut result = Vec::new();
        for dmeta in self.dmeta_maps(heads)? {
            for facets in self.maps(&dmeta, "facets", heads)? {
                result.extend(self.maps(&facets, &self.facet_key.to_string(), heads)?);
            }
        }
        Ok(result)
    }

    fn scalar(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
    ) -> Result<Option<ScalarValue>, ClaimHistoryError> {
        let values = self.doc.get_all_at(object, property, heads)?;
        self.account(heads, values.len())?;
        let mut result = None;
        for (value, _) in values {
            let Value::Scalar(scalar) = value else {
                return Err(ClaimHistoryError::MalformedWitness("metadata scalar"));
            };
            let bytes = match scalar.as_ref() {
                ScalarValue::Bytes(value) => value.len(),
                ScalarValue::Str(value) => value.len(),
                _ => 0,
            };
            self.native_budget.bytes = self
                .native_budget
                .bytes
                .checked_sub(bytes)
                .ok_or(ClaimHistoryError::ResourceRefused("metadata bytes"))?;
            if result
                .as_ref()
                .is_some_and(|previous| previous != scalar.as_ref())
            {
                return Err(ClaimHistoryError::MalformedWitness(
                    "conflicting metadata scalar",
                ));
            }
            result = Some(scalar.into_owned());
        }
        Ok(result)
    }

    fn text(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
    ) -> Result<String, ClaimHistoryError> {
        match self.scalar(object, property, heads)? {
            Some(ScalarValue::Str(value)) => Ok(value.to_string()),
            _ => Err(ClaimHistoryError::MalformedWitness("metadata text")),
        }
    }

    fn bytes(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
    ) -> Result<Option<Bytes>, ClaimHistoryError> {
        match self.scalar(object, property, heads)? {
            Some(ScalarValue::Bytes(value)) => Ok(Some(Bytes::from(value))),
            None => Ok(None),
            Some(_) => Err(ClaimHistoryError::MalformedWitness("metadata bytes")),
        }
    }

    fn uuid(text: &str) -> Result<Uuid7, ClaimHistoryError> {
        text.parse::<uuid::Uuid>()
            .map_err(|_| ClaimHistoryError::MalformedWitness("UUID text"))?
            .try_into()
            .map_err(|_| ClaimHistoryError::MalformedWitness("UUID version"))
    }

    fn read_signature(
        &mut self,
        marker: &ObjId,
        heads: &[ChangeHash],
    ) -> Result<FacetSignature, ClaimHistoryError> {
        let actor_uuid = Self::uuid(&self.text(marker, "actorUuid", heads)?)?;
        let bytes = self
            .bytes(marker, "signature", heads)?
            .ok_or(ClaimHistoryError::MalformedWitness("signature bytes"))?;
        let signature = SignatureBytes::try_from(bytes.as_slice())
            .map_err(|_| ClaimHistoryError::MalformedWitness("signature length"))?;
        let message = self.bytes(marker, "message", heads)?;
        let inherited = self.maps(marker, "inherited", heads)?;
        let inherited = match inherited.as_slice() {
            [] => None,
            [witness] => Some(InheritedWitness {
                origin_doc: self
                    .text(witness, "originDoc", heads)?
                    .parse()
                    .map_err(|_| ClaimHistoryError::MalformedWitness("origin document"))?,
                message: self
                    .bytes(witness, "message", heads)?
                    .ok_or(ClaimHistoryError::MalformedWitness("inherited message"))?,
                value: self.bytes(witness, "value", heads)?,
            }),
            _ => {
                return Err(ClaimHistoryError::MalformedWitness(
                    "conflicting inherited witnesses",
                ));
            }
        };
        let doc_uuid = Self::uuid(&self.text(marker, "docUuid", heads)?)?;
        Ok(FacetSignature {
            actor_uuid,
            signature,
            doc_uuid,
            message,
            inherited,
        })
    }

    fn actor_binding(
        &mut self,
        actor_uuid: Uuid7,
        heads: &[ChangeHash],
    ) -> Result<(MultibaseKey, Option<String>, Option<Bytes>), ClaimHistoryError> {
        let mut binding: Option<(MultibaseKey, Option<String>, Option<Bytes>)> = None;
        for dmeta in self.dmeta_maps(heads)? {
            for actors in self.maps(&dmeta, "actors", heads)? {
                for actor in self.maps(&actors, &actor_uuid.to_string(), heads)? {
                    let key = self
                        .text(&actor, "pubkey", heads)?
                        .parse::<MultibaseKey>()
                        .map_err(|_| ClaimHistoryError::MalformedWitness("actor public key"))?;
                    let url = match self.scalar(&actor, "actorUrl", heads)? {
                        Some(ScalarValue::Str(url)) => Some(url.to_string()),
                        None => None,
                        Some(_) => {
                            return Err(ClaimHistoryError::MalformedWitness("actor URL"));
                        }
                    };
                    let certificate = self.bytes(&actor, "delegation", heads)?;
                    let candidate = (key, url, certificate);
                    if binding
                        .as_ref()
                        .is_some_and(|binding| binding != &candidate)
                    {
                        return Err(ClaimHistoryError::MalformedWitness("rebound actor UUID"));
                    }
                    binding = Some(candidate);
                }
            }
        }
        // The binding a signature names is read at the change that introduced it, which no later
        // change can alter: a signature whose actor is unbound there never becomes verifiable.
        binding.ok_or(ClaimHistoryError::MalformedWitness("unbound actor"))
    }

    /// The document `uuid` is bound to in `branchUuids` at `heads`.
    fn document(&mut self, uuid: Uuid7, heads: &[ChangeHash]) -> Result<DocId, ClaimHistoryError> {
        let mut bound: Option<DocId> = None;
        for dmeta in self.dmeta_maps(heads)? {
            for index in self.maps(&dmeta, "branchUuids", heads)? {
                let Some(text) = self.scalar(&index, &uuid.to_string(), heads)? else {
                    continue;
                };
                let ScalarValue::Str(text) = text else {
                    return Err(ClaimHistoryError::MalformedWitness("document UUID binding"));
                };
                let document = text
                    .parse::<DocId>()
                    .map_err(|_| ClaimHistoryError::MalformedWitness("document UUID binding"))?;
                if bound.as_ref().is_some_and(|bound| *bound != document) {
                    return Err(ClaimHistoryError::MalformedWitness("rebound document UUID"));
                }
                bound = Some(document);
            }
        }
        bound.ok_or(ClaimHistoryError::MalformedWitness("unbound document UUID"))
    }

    fn aliases(&mut self, heads: &[ChangeHash]) -> Result<Vec<Uuid7>, ClaimHistoryError> {
        let mut aliases = Vec::new();
        let doc = self.doc;
        let target = self.facet_key.to_string();
        for dmeta in self.dmeta_maps(heads)? {
            for index in self.maps(&dmeta, "facetUuids", heads)? {
                let length = doc.length_at(&index, heads);
                self.account(heads, length)?;
                for item in doc.map_range_at(&index, .., heads) {
                    if !item.conflict {
                        self.account(heads, 1)?;
                        if matches!(
                            item.value,
                            automerge::ValueRef::Scalar(automerge::ScalarValueRef::Str(ref text))
                                if text.as_ref() == target.as_str()
                        ) {
                            aliases.push(Self::uuid(item.key.as_ref())?);
                        }
                        continue;
                    }
                    let values = doc.get_all_at(&index, item.key.as_ref(), heads)?;
                    self.account(heads, values.len())?;
                    let mut matches = false;
                    let mut other = false;
                    for (value, _) in values {
                        if value.to_str() == Some(target.as_str()) {
                            matches = true;
                        } else {
                            other = true;
                        }
                    }
                    if matches && other {
                        return Err(ClaimHistoryError::MalformedWitness("rebound facet UUID"));
                    }
                    if matches {
                        aliases.push(Self::uuid(item.key.as_ref())?);
                    }
                }
            }
        }
        aliases.sort();
        aliases.dedup();
        Ok(aliases)
    }

    fn facet_value(
        &mut self,
        heads: &[ChangeHash],
    ) -> Result<Option<NativeValue>, ClaimHistoryError> {
        let Some((value, facets)) = self.doc.get_at(automerge::ROOT, "facets", heads)? else {
            return Err(ClaimHistoryError::MalformedWitness("facets map"));
        };
        if value != Value::Object(ObjType::Map) {
            return Err(ClaimHistoryError::MalformedWitness("facets map"));
        }
        NativeValue::read_at_heads(
            self.doc,
            &facets,
            self.facet_key.to_string(),
            heads,
            &mut self.native_budget,
        )
        .map_err(ClaimHistoryError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::Prop;
    use automerge::transaction::Transactable;
    use automerge::transaction::Transaction;
    use big_cabinet_core::dmeta::CanonicalF64;
    use ed25519_dalek::Signer;
    use ed25519_dalek::SigningKey;
    use std::collections::BTreeMap;

    const FACET: &str = "org.example.note/main";
    const DMETA: &str = "org.example.daybook.dmeta/main";

    struct Fixture {
        doc: Automerge,
        document: DocId,
        doc_uuid: Uuid7,
        facet: FacetKey,
        alias: Uuid7,
        actor: Uuid7,
        key: SigningKey,
    }

    struct Paths {
        facets: ObjId,
        dmeta: ObjId,
        metadata: ObjId,
        signatures: ObjId,
        aliases: ObjId,
    }

    impl Fixture {
        fn new() -> Self {
            let mut fixture = Self {
                doc: Automerge::new(),
                document: DocId::new([7; 32]),
                doc_uuid: Reconstruction::uuid("01940000-0000-7000-8000-000000000003").unwrap(),
                facet: FacetKey::from_text("org.example.note", "main").unwrap(),
                alias: Reconstruction::uuid("01940000-0000-7000-8000-000000000001").unwrap(),
                actor: Reconstruction::uuid("01940000-0000-7000-8000-000000000002").unwrap(),
                key: SigningKey::from_bytes(&[9; 32]),
            };
            let mut tx = fixture.doc.transaction();
            let facets = tx
                .put_object(automerge::ROOT, "facets", ObjType::Map)
                .unwrap();
            let dmeta = tx.put_object(&facets, DMETA, ObjType::Map).unwrap();
            tx.put(&dmeta, "createdAt", ScalarValue::Timestamp(0))
                .unwrap();
            tx.put_object(&dmeta, "updatedAt", ObjType::List).unwrap();
            let aliases = tx.put_object(&dmeta, "facetUuids", ObjType::Map).unwrap();
            tx.put(&aliases, fixture.alias.to_string(), FACET).unwrap();
            let documents = tx.put_object(&dmeta, "branchUuids", ObjType::Map).unwrap();
            tx.put(
                &documents,
                fixture.doc_uuid.to_string(),
                fixture.document.to_string(),
            )
            .unwrap();
            let actors = tx.put_object(&dmeta, "actors", ObjType::Map).unwrap();
            let actor = tx
                .put_object(&actors, fixture.actor.to_string(), ObjType::Map)
                .unwrap();
            let public_key =
                MultibaseKey::try_from(fixture.key.verifying_key().as_bytes().as_slice()).unwrap();
            tx.put(&actor, "pubkey", public_key.to_string()).unwrap();
            tx.put(&actor, "actorUrl", "db+actor://node/test").unwrap();
            let metadata = tx.put_object(&dmeta, "facets", ObjType::Map).unwrap();
            let metadata = tx.put_object(&metadata, FACET, ObjType::Map).unwrap();
            tx.put_object(&metadata, "updatedAt", ObjType::List)
                .unwrap();
            tx.put_object(&metadata, "deletedAt", ObjType::List)
                .unwrap();
            tx.put_object(&metadata, "signatures", ObjType::List)
                .unwrap();
            tx.put(&facets, FACET, "seed").unwrap();
            tx.commit();
            fixture
        }

        fn paths(&self) -> Paths {
            let (_, facets) = self.doc.get(automerge::ROOT, "facets").unwrap().unwrap();
            let (_, dmeta) = self.doc.get(&facets, DMETA).unwrap().unwrap();
            let (_, aliases) = self.doc.get(&dmeta, "facetUuids").unwrap().unwrap();
            let (_, metadata) = self.doc.get(&dmeta, "facets").unwrap().unwrap();
            let (_, metadata) = self.doc.get(&metadata, FACET).unwrap().unwrap();
            let (_, signatures) = self.doc.get(&metadata, "signatures").unwrap().unwrap();
            Paths {
                facets,
                dmeta,
                metadata,
                signatures,
                aliases,
            }
        }

        fn signed(&self, value: Option<&NativeValue>) -> SignedFacetClaim {
            let mut state = Reconstruction::new(&self.doc, &self.facet, HistoryBudget::default());
            let claim = FacetClaim {
                origin_document: self.document.clone(),
                facet_key: self.facet.clone(),
                chosen_uuid: self.alias,
                actor_uuid: self.actor,
                operation: if value.is_some() {
                    ClaimOperation::Set
                } else {
                    ClaimOperation::Remove
                },
                value_cid: value.map(|value| cid_of(&encode_native(value))),
                predecessors: state.predecessor_ids(&self.doc.get_heads()).unwrap(),
            };
            SignedFacetClaim {
                signature: SignatureBytes::try_from(
                    self.key
                        .sign(&claim.signing_bytes().unwrap())
                        .to_bytes()
                        .as_slice(),
                )
                .unwrap(),
                actor_public_key: MultibaseKey::try_from(
                    self.key.verifying_key().as_bytes().as_slice(),
                )
                .unwrap(),
                claim,
            }
        }

        fn install(
            &mut self,
            value: Option<&NativeValue>,
            signed: &SignedFacetClaim,
        ) -> (ObjId, ChangeHash) {
            self.install_entry(
                value,
                &FacetSignature {
                    actor_uuid: signed.claim.actor_uuid,
                    doc_uuid: self.doc_uuid,
                    signature: signed.signature.clone(),
                    message: None,
                    inherited: None,
                },
            )
        }

        fn install_entry(
            &mut self,
            value: Option<&NativeValue>,
            entry: &FacetSignature,
        ) -> (ObjId, ChangeHash) {
            let paths = self.paths();
            let mut tx = self.doc.transaction();
            match value {
                Some(value) => put_native(&mut tx, &paths.facets, FACET.into(), value),
                None => tx.delete(&paths.facets, FACET).unwrap(),
            }
            for index in (0..tx.length(&paths.signatures)).rev() {
                tx.delete(&paths.signatures, index).unwrap();
            }
            let marker = tx
                .insert_object(&paths.signatures, 0, ObjType::Map)
                .unwrap();
            tx.put(&marker, "actorUuid", entry.actor_uuid.to_string())
                .unwrap();
            tx.put(&marker, "docUuid", entry.doc_uuid.to_string())
                .unwrap();
            tx.put(&marker, "signature", entry.signature.as_bytes().to_vec())
                .unwrap();
            if let Some(message) = &entry.message {
                tx.put(&marker, "message", message.as_slice().to_vec())
                    .unwrap();
            }
            if let Some(witness) = &entry.inherited {
                let inherited = tx.put_object(&marker, "inherited", ObjType::Map).unwrap();
                tx.put(&inherited, "originDoc", witness.origin_doc.to_string())
                    .unwrap();
                tx.put(&inherited, "message", witness.message.as_slice().to_vec())
                    .unwrap();
                if let Some(value) = &witness.value {
                    tx.put(&inherited, "value", value.as_slice().to_vec())
                        .unwrap();
                }
            }
            let hash = tx.commit().0.unwrap();
            (marker, hash)
        }

        fn write(&mut self, value: Option<&NativeValue>) -> (ObjId, ChangeHash, SignedFacetClaim) {
            let signed = self.signed(value);
            let (marker, hash) = self.install(value, &signed);
            (marker, hash, signed)
        }

        fn reconstruct(&self, marker: &ObjId) -> ClaimHistoryResult {
            reconstruct_claim_for_marker(&self.doc, &self.facet, marker, HistoryBudget::default())
        }
    }

    fn put_native(tx: &mut Transaction<'_>, object: &ObjId, prop: Prop, value: &NativeValue) {
        let scalar = match value {
            NativeValue::Null => ScalarValue::Null,
            NativeValue::Bool(value) => ScalarValue::Boolean(*value),
            NativeValue::Int(value) => ScalarValue::Int(*value),
            NativeValue::Uint(value) => ScalarValue::Uint(*value),
            NativeValue::Float(value) => ScalarValue::F64(value.get()),
            NativeValue::Str(value) => ScalarValue::Str(value.as_str().into()),
            NativeValue::Bytes(value) => ScalarValue::Bytes(value.clone()),
            NativeValue::Timestamp(value) => ScalarValue::Timestamp(*value),
            NativeValue::Counter(value) => ScalarValue::Counter((*value).into()),
            NativeValue::Text(value) => {
                let text = tx.put_object(object, prop, ObjType::Text).unwrap();
                tx.splice_text(&text, 0, 0, value).unwrap();
                return;
            }
            NativeValue::Map(values) => {
                let map = tx.put_object(object, prop, ObjType::Map).unwrap();
                for (key, value) in values {
                    put_native(tx, &map, key.as_str().into(), value);
                }
                return;
            }
            NativeValue::List(values) => {
                let list = tx.put_object(object, prop, ObjType::List).unwrap();
                for (index, value) in values.iter().enumerate() {
                    tx.insert(&list, index, ScalarValue::Null).unwrap();
                    put_native(tx, &list, index.into(), value);
                }
                return;
            }
        };
        tx.put(object, prop, scalar).unwrap();
    }

    fn accepted(result: ClaimHistoryResult) -> ReconstructedClaim {
        match result {
            ClaimHistoryResult::Accepted(claim) => *claim,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn introduction_uses_historical_value_alias_and_actor_binding() {
        let mut fixture = Fixture::new();
        let parents = fixture.doc.get_heads();
        let (marker, hash, signed) = fixture.write(Some(&NativeValue::Str("first".into())));
        let paths = fixture.paths();
        let mut tx = fixture.doc.transaction();
        tx.put(&paths.facets, FACET, "later").unwrap();
        tx.put(
            &paths.aliases,
            "01930000-0000-7000-8000-000000000001",
            FACET,
        )
        .unwrap();
        let (_, actors) = tx.get(&paths.dmeta, "actors").unwrap().unwrap();
        let (_, actor) = tx.get(&actors, fixture.actor.to_string()).unwrap().unwrap();
        tx.put(&actor, "pubkey", "invalid-later-binding").unwrap();
        tx.commit();
        let reconstructed = accepted(fixture.reconstruct(&marker));
        assert_eq!(reconstructed.claim, signed.claim);
        assert_eq!(reconstructed.actor_public_key, signed.actor_public_key);
        assert_eq!(reconstructed.introduction.parents, parents);
        assert_eq!(reconstructed.introduction.resulting_heads, vec![hash]);
        assert_eq!(
            accepted(reconstruct_claim(
                &fixture.doc,
                &fixture.facet,
                hash,
                HistoryBudget::default()
            )),
            reconstructed
        );
    }

    #[test]
    fn native_kinds_are_signed_without_schema_and_survive_save_load() {
        let mut fixture = Fixture::new();
        let value = NativeValue::Map(BTreeMap::from([
            ("bytes".into(), NativeValue::Bytes(vec![0, 255])),
            ("text".into(), NativeValue::Text("é𐐀".into())),
            ("counter".into(), NativeValue::Counter(3)),
            ("timestamp".into(), NativeValue::Timestamp(-7)),
            ("uint".into(), NativeValue::Uint(u64::MAX)),
            (
                "float".into(),
                NativeValue::List(vec![
                    NativeValue::Float(CanonicalF64::new(f64::NAN)),
                    NativeValue::Float(CanonicalF64::new(-0.0)),
                ]),
            ),
        ]));
        let (marker, _, signed) = fixture.write(Some(&value));
        fixture.doc = Automerge::load(&fixture.doc.save()).unwrap();
        assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
    }

    #[test]
    fn slot_presence_not_unsigned_deleted_at_decides_operation() {
        let mut fixture = Fixture::new();
        let metadata = fixture.paths().metadata;
        let mut tx = fixture.doc.transaction();
        let (_, deleted) = tx.get(&metadata, "deletedAt").unwrap().unwrap();
        tx.insert(&deleted, 0, ScalarValue::Timestamp(5)).unwrap();
        tx.commit();
        let (null_marker, _, null_signed) = fixture.write(Some(&NativeValue::Null));
        assert_eq!(
            accepted(fixture.reconstruct(&null_marker)).claim,
            null_signed.claim
        );
        let (remove_marker, _, removed) = fixture.write(None);
        assert_eq!(
            accepted(fixture.reconstruct(&remove_marker)).claim,
            removed.claim
        );
        assert_eq!(removed.claim.operation, ClaimOperation::Remove);
        assert_eq!(removed.claim.value_cid, None);
    }

    #[test]
    fn invalid_predecessor_is_signed_context_not_a_recursive_trust_gate() {
        let mut fixture = Fixture::new();
        fixture.write(Some(&NativeValue::Str("first".into())));
        let value = NativeValue::Str("invalid".into());
        let mut invalid = fixture.signed(Some(&value));
        invalid.signature = SignatureBytes::try_from([0_u8; 64].as_slice()).unwrap();
        let (invalid_marker, _) = fixture.install(Some(&value), &invalid);
        assert_eq!(
            fixture.reconstruct(&invalid_marker),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
        );
        let invalid_id = invalid.signature_id().unwrap();
        let (marker, _, signed) = fixture.write(Some(&NativeValue::Str("replacement".into())));
        assert_eq!(signed.claim.predecessors, vec![invalid_id]);
        assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
    }

    #[test]
    fn competing_bad_and_incomplete_entries_do_not_erase_accepted_evidence() {
        let mut fixture = Fixture::new();
        let (good, _, signed) = fixture.write(Some(&NativeValue::Str("good".into())));
        let paths = fixture.paths();
        let mut tx = fixture.doc.transaction();
        let bad = tx
            .insert_object(&paths.signatures, 1, ObjType::Map)
            .unwrap();
        tx.put(&bad, "actorUuid", fixture.actor.to_string())
            .unwrap();
        tx.put(&bad, "docUuid", fixture.doc_uuid.to_string())
            .unwrap();
        tx.put(&bad, "signature", vec![0_u8; 64]).unwrap();
        let incomplete = tx
            .insert_object(&paths.signatures, 2, ObjType::Map)
            .unwrap();
        tx.put(&incomplete, "signature", vec![0_u8; 64]).unwrap();
        tx.commit();
        let claims = reconstruct_current_claims(
            &fixture.doc,
            &fixture.facet,
            &fixture.doc.get_heads(),
            HistoryBudget::default(),
        )
        .unwrap();
        assert_eq!(claims.len(), 3);
        assert_eq!(
            accepted(
                claims
                    .iter()
                    .find(|entry| entry.marker == good)
                    .unwrap()
                    .result
                    .clone()
            )
            .claim,
            signed.claim
        );
        assert_eq!(
            claims
                .iter()
                .find(|entry| entry.marker == bad)
                .unwrap()
                .result,
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
        );
        assert_eq!(
            claims
                .iter()
                .find(|entry| entry.marker == incomplete)
                .unwrap()
                .result,
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness("metadata text"))
        );
    }

    #[test]
    fn mutated_signature_is_not_backdated_by_frontier_cache() {
        let mut fixture = Fixture::new();
        let (marker, _, signed) = fixture.write(Some(&NativeValue::Str("first".into())));
        let paths = fixture.paths();
        let mut tx = fixture.doc.transaction();
        tx.put(&paths.facets, FACET, "later").unwrap();
        tx.put(&marker, "signature", vec![0_u8; 64]).unwrap();
        tx.commit();
        assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
        let entries = reconstruct_current_claims(
            &fixture.doc,
            &fixture.facet,
            &fixture.doc.get_heads(),
            HistoryBudget::default(),
        )
        .unwrap();
        assert_eq!(
            entries,
            vec![ClaimHistoryEntry {
                marker,
                result: ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                    "signature entry changed after introduction"
                ))
            }]
        );
    }

    #[test]
    fn unrelated_marker_is_not_a_signature_for_this_facet() {
        let mut fixture = Fixture::new();
        let mut tx = fixture.doc.transaction();
        let list = tx
            .put_object(automerge::ROOT, "unrelated", ObjType::List)
            .unwrap();
        let marker = tx.insert_object(&list, 0, ObjType::Map).unwrap();
        tx.commit();
        assert_eq!(
            fixture.reconstruct(&marker),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                "marker outside target signature set"
            ))
        );
    }

    #[test]
    fn distinct_markers_in_one_change_have_independent_verdicts() {
        let mut fixture = Fixture::new();
        let value = NativeValue::Str("same".into());
        let signed = fixture.signed(Some(&value));
        let paths = fixture.paths();
        let mut tx = fixture.doc.transaction();
        tx.put(&paths.facets, FACET, "same").unwrap();
        for index in 0..2 {
            let marker = tx
                .insert_object(&paths.signatures, index, ObjType::Map)
                .unwrap();
            tx.put(&marker, "actorUuid", fixture.actor.to_string())
                .unwrap();
            tx.put(&marker, "docUuid", fixture.doc_uuid.to_string())
                .unwrap();
            tx.put(&marker, "signature", signed.signature.as_bytes().to_vec())
                .unwrap();
        }
        let hash = tx.commit().0.unwrap();
        let claims = reconstruct_current_claims(
            &fixture.doc,
            &fixture.facet,
            &[hash],
            HistoryBudget::default(),
        )
        .unwrap();
        assert_eq!(claims.len(), 2);
        assert_ne!(claims[0].marker, claims[1].marker);
        for entry in claims {
            assert_eq!(accepted(entry.result).claim, signed.claim);
        }
        assert_eq!(
            reconstruct_claim(&fixture.doc, &fixture.facet, hash, HistoryBudget::default()),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                "one signature marker at introduction"
            ))
        );
    }

    #[test]
    fn concurrent_writers_reconstruct_their_own_transaction_views() {
        let mut left = Fixture::new();
        let mut right = Fixture {
            doc: left.doc.fork(),
            document: left.document.clone(),
            doc_uuid: left.doc_uuid,
            facet: left.facet.clone(),
            alias: left.alias,
            actor: left.actor,
            key: left.key.clone(),
        };
        let (_, _, left_signed) = left.write(Some(&NativeValue::Str("left".into())));
        let (_, _, right_signed) = right.write(Some(&NativeValue::Str("right".into())));
        left.doc.merge(&mut right.doc).unwrap();
        let claims = reconstruct_current_claims(
            &left.doc,
            &left.facet,
            &left.doc.get_heads(),
            HistoryBudget::default(),
        )
        .unwrap();
        let mut actual: Vec<_> = claims
            .into_iter()
            .map(|entry| accepted(entry.result).claim)
            .collect();
        let mut expected = vec![left_signed.claim, right_signed.claim];
        actual.sort_by(|left, right| left.value_cid.cmp(&right.value_cid));
        expected.sort_by(|left, right| left.value_cid.cmp(&right.value_cid));
        assert_eq!(actual, expected);
    }

    #[test]
    fn carried_message_must_agree_with_reconstructible_context() {
        let mut fixture = Fixture::new();
        let value = NativeValue::Str("value".into());
        let signed = fixture.signed(Some(&value));
        let mut entry = FacetSignature {
            actor_uuid: fixture.actor,
            doc_uuid: fixture.doc_uuid,
            signature: signed.signature.clone(),
            message: Some(Bytes::from(signed.claim.signing_bytes().unwrap())),
            inherited: None,
        };
        let (marker, _) = fixture.install_entry(Some(&value), &entry);
        assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim);
        let signed = fixture.signed(Some(&value));
        entry.signature = signed.signature;
        entry.message = Some(Bytes::from(vec![1, 2, 3]));
        let (marker, _) = fixture.install_entry(Some(&value), &entry);
        assert_eq!(
            fixture.reconstruct(&marker),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                "carried and reconstructed messages disagree"
            ))
        );
    }

    utils_rs::table_tests! {
        doc_uuid_binding_is_read_at_the_introduction,
        (binding, expected),
        {
            let expected: Option<&str> = expected;
            let mut fixture = Fixture::new();
            let value = NativeValue::Str("value".into());
            let unbound = Reconstruction::uuid("01940000-0000-7000-8000-000000000004").unwrap();
            let other = DocId::new([8; 32]);
            let (_, documents) = fixture
                .doc
                .get(&fixture.paths().dmeta, "branchUuids")
                .unwrap()
                .unwrap();
            let doc_uuid = match binding {
                "bound" | "rebound later" => fixture.doc_uuid,
                "unbound" | "bound later" => unbound,
                "conflicting" => {
                    let mut peer = fixture.doc.fork();
                    let third = DocId::new([9; 32]);
                    for (doc, document) in [(&mut peer, &other), (&mut fixture.doc, &third)] {
                        let mut tx = doc.transaction();
                        tx.put(&documents, fixture.doc_uuid.to_string(), document.to_string()).unwrap();
                        tx.commit();
                    }
                    fixture.doc.merge(&mut peer).unwrap();
                    fixture.doc_uuid
                }
                other => panic!("unknown case: {other}"),
            };
            let signed = fixture.signed(Some(&value));
            let entry = FacetSignature {
                actor_uuid: fixture.actor,
                doc_uuid,
                signature: signed.signature.clone(),
                message: None,
                inherited: None,
            };
            let (marker, _) = fixture.install_entry(Some(&value), &entry);
            let later = match binding {
                "bound later" => Some((unbound, &fixture.document)),
                "rebound later" => Some((fixture.doc_uuid, &other)),
                _ => None,
            };
            if let Some((uuid, document)) = later {
                let mut tx = fixture.doc.transaction();
                tx.put(&documents, uuid.to_string(), document.to_string()).unwrap();
                tx.commit();
            }
            match expected {
                None => assert_eq!(accepted(fixture.reconstruct(&marker)).claim, signed.claim),
                Some(reason) => assert_eq!(
                    fixture.reconstruct(&marker),
                    ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(reason)),
                ),
            }
        }
    }

    doc_uuid_binding_is_read_at_the_introduction! {
        bound: ("bound", None),
        rebinding_after_the_introduction_changes_nothing: ("rebound later", None),
        unbound: ("unbound", Some("unbound document UUID")),
        binding_after_the_introduction_is_too_late: ("bound later", Some("unbound document UUID")),
        conflicting_bindings: ("conflicting", Some("conflicting metadata scalar")),
    }

    #[test]
    fn inherited_message_keeps_origin_authorship_distinct_from_the_compacted_value() {
        let origin = Fixture::new();
        let value = NativeValue::Text("historical".into());
        let signed = origin.signed(Some(&value));
        let mut branch = Fixture::new();
        branch.document = DocId::new([8; 32]);
        let entry = FacetSignature {
            actor_uuid: origin.actor,
            doc_uuid: branch.doc_uuid,
            signature: signed.signature.clone(),
            message: None,
            inherited: Some(InheritedWitness {
                origin_doc: origin.document,
                message: Bytes::from(signed.claim.signing_bytes().unwrap()),
                value: Some(Bytes::from(encode_native(&value))),
            }),
        };
        let (marker, _) = branch.install_entry(
            Some(&NativeValue::Str("different compacted value".into())),
            &entry,
        );
        assert_eq!(accepted(branch.reconstruct(&marker)).claim, signed.claim);
        let mut wrong = entry;
        wrong.inherited.as_mut().unwrap().value = Some(Bytes::from(encode_native(
            &NativeValue::Str("not signed".into()),
        )));
        let (marker, _) = branch.install_entry(Some(&NativeValue::Str("different".into())), &wrong);
        assert_eq!(
            branch.reconstruct(&marker),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                "inherited context"
            ))
        );
    }

    #[test]
    fn reconstruction_does_not_recurse_into_ancestor_claims() {
        let mut fixture = Fixture::new();
        let mut last_marker = None;
        for step in 0..100 {
            let (marker, _, _) = fixture.write(Some(&NativeValue::Str(format!("step-{step}"))));
            last_marker = Some(marker);
        }
        let marker = last_marker.unwrap();
        accepted(reconstruct_claim_for_marker(
            &fixture.doc,
            &fixture.facet,
            &marker,
            HistoryBudget {
                max_claims: 1,
                max_depth: 1,
                ..HistoryBudget::default()
            },
        ));
    }

    utils_rs::table_tests! {
        atomic_change_delivery,
        order,
        {
            let mut fixture = Fixture::new();
            fixture.write(Some(&NativeValue::Str("first".into())));
            fixture.write(Some(&NativeValue::Str("second".into())));
            let changes = fixture.doc.get_changes(&[]);
            assert_eq!(changes.len(), 3);
            let mut receiver = Automerge::new();
            let mut delivered = [false; 3];
            for index in order {
                receiver.apply_changes([changes[index].clone()]).unwrap();
                delivered[index] = true;
                let applied = delivered.iter().take_while(|seen| **seen).count();
                assert_eq!(receiver.get_changes(&[]).len(), applied);
                receiver = Automerge::load(&receiver.save()).unwrap();
                let entries = reconstruct_current_claims(
                    &receiver,
                    &fixture.facet,
                    &receiver.get_heads(),
                    HistoryBudget::default(),
                ).unwrap();
                assert_eq!(entries.len(), usize::from(applied >= 2));
                for entry in entries {
                    let claim = accepted(entry.result);
                    assert_eq!(
                        receiver.hash_for_opid(&entry.marker),
                        Some(claim.introduction.introduction_hash),
                    );
                    assert!(receiver.get_change_by_hash(&claim.introduction.introduction_hash).is_some());
                    assert_eq!(claim.claim.chosen_uuid, fixture.alias);
                    assert_eq!(claim.claim.actor_uuid, fixture.actor);
                }
            }
            assert_eq!(receiver.get_heads(), fixture.doc.get_heads());
        }
    }

    atomic_change_delivery! {
        causal_order: [0, 1, 2],
        reversed: [2, 1, 0],
        last_before_first: [2, 0, 1],
        middle_before_first: [1, 0, 2],
        first_then_last: [0, 2, 1],
        first_arrives_last: [1, 2, 0],
    }

    #[test]
    fn resource_refusals_remain_pending_and_unknown_heads_are_malformed() {
        let mut fixture = Fixture::new();
        let (marker, _, _) = fixture.write(Some(&NativeValue::Str("bounded".into())));
        for budget in [
            HistoryBudget {
                max_claims: 0,
                ..HistoryBudget::default()
            },
            HistoryBudget {
                max_entries_per_frontier: 0,
                ..HistoryBudget::default()
            },
            HistoryBudget {
                max_value_nodes: 0,
                ..HistoryBudget::default()
            },
            HistoryBudget {
                max_bytes: 0,
                ..HistoryBudget::default()
            },
            HistoryBudget {
                max_depth: 0,
                ..HistoryBudget::default()
            },
        ] {
            assert!(matches!(
                reconstruct_claim_for_marker(&fixture.doc, &fixture.facet, &marker, budget),
                ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(_))
            ));
        }
        assert_eq!(
            reconstruct_current_claims(
                &fixture.doc,
                &fixture.facet,
                &[ChangeHash([255; 32])],
                HistoryBudget::default()
            ),
            Err(ClaimHistoryError::MalformedWitness("unknown heads"))
        );
    }
}
