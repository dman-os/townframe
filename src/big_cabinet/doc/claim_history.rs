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
mod tests;
