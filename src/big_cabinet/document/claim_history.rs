use automerge::{Automerge, ChangeHash, ObjId, ObjType, ReadDoc, ScalarValue, Value};
use big_cabinet_core::FacetKey;
use big_cabinet_core::dmeta::{
    ClaimOperation, DocumentId, FacetClaim, FacetSignature, SignedFacetClaim, value_cid,
};
use serde_json::{Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Limits history hydration without turning refusal into a cryptographic failure.
#[derive(Debug, Clone, Copy)]
pub struct HistoryBudget {
    pub max_claims: usize,
    pub max_entries_per_frontier: usize,
    pub max_value_nodes: usize,
}

impl Default for HistoryBudget {
    fn default() -> Self {
        Self {
            max_claims: 256,
            max_entries_per_frontier: 256,
            max_value_nodes: 16_384,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimHistoryError {
    MissingEvidence(&'static str),
    ResourceRefused(&'static str),
    MalformedWitness(&'static str),
    InvalidSignature,
    Automerge(String),
}

impl ClaimHistoryError {
    fn pending(&self) -> bool {
        matches!(
            self,
            Self::MissingEvidence(_) | Self::ResourceRefused(_) | Self::Automerge(_)
        )
    }
}

/// The result intentionally keeps pending evidence distinct from a proven bad witness.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimHistoryResult {
    Accepted(Box<ReconstructedClaim>),
    Pending(ClaimHistoryError),
    ProvenForgery(ClaimHistoryError),
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
    pub claim: FacetClaim,
}

/// Map the signature list operation that introduced a marker to its Automerge change.
/// Automerge's operation-to-change index is the authoritative assignment; no later scan of the
/// whole history is needed and a missing index remains pending evidence.
pub fn reconstruct_claim_for_marker(
    doc: &Automerge,
    origin_document: &DocumentId,
    facet_key: &FacetKey,
    marker: &ObjId,
    budget: HistoryBudget,
) -> ClaimHistoryResult {
    let Some(introduction_hash) = doc.hash_for_opid(marker) else {
        return ClaimHistoryResult::Pending(ClaimHistoryError::MissingEvidence(
            "signature marker introduction hash",
        ));
    };
    let mut state = Reconstruction {
        doc,
        origin_document,
        facet_key,
        budget,
        claims_seen: 0,
        value_nodes: 0,
        cache: HashMap::new(),
        signature_cache: HashMap::new(),
        hydrated_signatures: HashMap::new(),
        frontier_entries: HashMap::new(),
        active: HashSet::new(),
    };
    match state.claim_for_entry(introduction_hash, marker.clone()) {
        Ok(claim) => ClaimHistoryResult::Accepted(Box::new(claim)),
        Err(error) if error.pending() => ClaimHistoryResult::Pending(error),
        Err(error) => ClaimHistoryResult::ProvenForgery(error),
    }
}

pub fn reconstruct_claim(
    doc: &Automerge,
    origin_document: &DocumentId,
    facet_key: &FacetKey,
    introduction_hash: ChangeHash,
    budget: HistoryBudget,
) -> ClaimHistoryResult {
    let mut state = Reconstruction {
        doc,
        origin_document,
        facet_key,
        budget,
        claims_seen: 0,
        value_nodes: 0,
        cache: HashMap::new(),
        signature_cache: HashMap::new(),
        hydrated_signatures: HashMap::new(),
        frontier_entries: HashMap::new(),
        active: HashSet::new(),
    };
    match state.claim_for_hash(introduction_hash) {
        Ok(claim) => ClaimHistoryResult::Accepted(Box::new(claim)),
        Err(error) if error.pending() => ClaimHistoryResult::Pending(error),
        Err(error) => ClaimHistoryResult::ProvenForgery(error),
    }
}

/// Recover signatures visible now. The list is a competing set, not an audit log; older entries
/// remain discoverable by passing their introduction hash to [`reconstruct_claim`].
pub fn reconstruct_current_claims(
    doc: &Automerge,
    origin_document: &DocumentId,
    facet_key: &FacetKey,
    heads: &[ChangeHash],
    budget: HistoryBudget,
) -> Result<Vec<ReconstructedClaim>, ClaimHistoryError> {
    let mut state = Reconstruction {
        doc,
        origin_document,
        facet_key,
        budget,
        claims_seen: 0,
        value_nodes: 0,
        cache: HashMap::new(),
        signature_cache: HashMap::new(),
        hydrated_signatures: HashMap::new(),
        frontier_entries: HashMap::new(),
        active: HashSet::new(),
    };
    let entries = state.signature_entries(heads)?;
    let mut claims = Vec::with_capacity(entries.len());
    let mut entries_seen = HashSet::new();
    for (marker, hash, _) in entries {
        if entries_seen.insert((hash, marker.clone())) {
            claims.push(state.claim_for_entry(hash, marker)?);
        }
    }
    Ok(claims)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FrontierKey(Vec<ChangeHash>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SignatureKey {
    hash: ChangeHash,
    marker: ObjId,
}

fn frontier_key(heads: &[ChangeHash]) -> FrontierKey {
    let mut heads = heads.to_vec();
    heads.sort();
    heads.dedup();
    FrontierKey(heads)
}

struct Reconstruction<'a> {
    doc: &'a Automerge,
    origin_document: &'a DocumentId,
    facet_key: &'a FacetKey,
    budget: HistoryBudget,
    claims_seen: usize,
    value_nodes: usize,
    cache: HashMap<SignatureKey, ReconstructedClaim>,
    signature_cache: HashMap<FrontierKey, Vec<(ObjId, ChangeHash, FacetSignature)>>,
    hydrated_signatures: HashMap<SignatureKey, FacetSignature>,
    frontier_entries: HashMap<FrontierKey, usize>,
    active: HashSet<ChangeHash>,
}

fn account_frontier_values(
    frontier_entries: &mut HashMap<FrontierKey, usize>,
    budget: HistoryBudget,
    heads: &[ChangeHash],
    count: usize,
) -> Result<(), ClaimHistoryError> {
    let key = frontier_key(heads);
    let used = frontier_entries.entry(key).or_default();
    let total = used
        .checked_add(count)
        .ok_or(ClaimHistoryError::ResourceRefused(
            "signature frontier budget exceeded",
        ))?;
    if total > budget.max_entries_per_frontier {
        return Err(ClaimHistoryError::ResourceRefused(
            "signature frontier budget exceeded",
        ));
    }
    *used = total;
    Ok(())
}

impl Reconstruction<'_> {
    fn claim_for_hash(
        &mut self,
        hash: ChangeHash,
    ) -> Result<ReconstructedClaim, ClaimHistoryError> {
        let change =
            self.doc
                .get_change_by_hash(&hash)
                .ok_or(ClaimHistoryError::MissingEvidence(
                    "signature introduction change",
                ))?;
        let mut resulting_heads = change.deps().to_vec();
        resulting_heads.push(hash);
        let markers: Vec<_> = self
            .signature_entries(&resulting_heads)?
            .into_iter()
            .filter(|(_, entry_hash, _)| *entry_hash == hash)
            .map(|(marker, _, _)| marker)
            .collect();
        let [marker] = markers.as_slice() else {
            return Err(if markers.is_empty() {
                ClaimHistoryError::MissingEvidence("signature entry at introduction")
            } else {
                ClaimHistoryError::MissingEvidence(
                    "multiple signature markers in one introduction change",
                )
            });
        };
        self.claim_for_entry(hash, marker.clone())
    }

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
        if !self.active.insert(hash) {
            return Err(ClaimHistoryError::MalformedWitness(
                "cyclic signature ancestry",
            ));
        }
        self.claims_seen += 1;
        if self.claims_seen > self.budget.max_claims {
            self.active.remove(&hash);
            return Err(ClaimHistoryError::ResourceRefused(
                "claim history budget exceeded",
            ));
        }
        let result = self.claim_for_entry_uncached(hash, marker);
        self.active.remove(&hash);
        if let Ok(claim) = &result {
            self.cache.insert(key, claim.clone());
        }
        result
    }

    fn claim_for_entry_uncached(
        &mut self,
        hash: ChangeHash,
        marker: ObjId,
    ) -> Result<ReconstructedClaim, ClaimHistoryError> {
        let change =
            self.doc
                .get_change_by_hash(&hash)
                .ok_or(ClaimHistoryError::MissingEvidence(
                    "signature introduction change",
                ))?;
        let parents = change.deps().to_vec();
        let mut resulting_heads = parents.clone();
        resulting_heads.push(hash);
        let signature_key = SignatureKey {
            hash,
            marker: marker.clone(),
        };
        let signature = if let Some(signature) = self.hydrated_signatures.get(&signature_key) {
            signature.clone()
        } else {
            let signature = self.read_signature(&marker, &resulting_heads)?;
            self.hydrated_signatures
                .insert(signature_key, signature.clone());
            signature
        };
        let chosen_uuid = self.read_chosen_uuid(&resulting_heads)?;
        let operation = self.read_operation(&resulting_heads)?;
        let value_cid = match operation {
            ClaimOperation::Set => Some(
                value_cid(&self.read_facet_value(&resulting_heads)?).map_err(|_| {
                    ClaimHistoryError::MalformedWitness("facet value cannot be canonicalized")
                })?,
            ),
            ClaimOperation::Remove => None,
        };
        let predecessors = self.accepted_predecessors(&parents)?;
        let claim = FacetClaim {
            origin_document: self.origin_document.clone(),
            facet_key: self.facet_key.clone(),
            chosen_uuid,
            operation,
            value_cid,
            predecessors,
        };
        let signed = SignedFacetClaim::from_signature(
            claim.clone(),
            signature.actor_public_key.clone(),
            signature.signature.clone(),
        )
        .map_err(|_| ClaimHistoryError::InvalidSignature)?;
        Ok(ReconstructedClaim {
            introduction: SignatureIntroduction {
                marker,
                introduction_hash: hash,
                parents,
                resulting_heads,
                automerge_actor: change.actor_id().clone(),
            },
            signature,
            claim: signed.claim,
        })
    }

    fn accepted_predecessors(
        &mut self,
        parents: &[ChangeHash],
    ) -> Result<Vec<Vec<u8>>, ClaimHistoryError> {
        let entries = self.signature_entries(parents)?;
        let mut hashes = HashSet::new();
        let mut predecessor_ids = Vec::new();
        for (marker, hash, _) in entries {
            if !hashes.insert((hash, marker.clone())) {
                continue;
            }
            match self.claim_for_entry(hash, marker) {
                Ok(previous) => {
                    let signed = SignedFacetClaim::from_signature(
                        previous.claim,
                        previous.signature.actor_public_key,
                        previous.signature.signature,
                    );
                    if let Ok(signed) = signed {
                        predecessor_ids.push(
                            signed.signature_id().map_err(|_| {
                                ClaimHistoryError::MalformedWitness("predecessor ID")
                            })?,
                        );
                    }
                }
                Err(ClaimHistoryError::InvalidSignature)
                | Err(ClaimHistoryError::MalformedWitness(_)) => {
                    // Visibility alone does not make an invalid entry an accepted predecessor.
                }
                Err(error) => return Err(error),
            }
        }
        predecessor_ids.sort();
        predecessor_ids.dedup();
        Ok(predecessor_ids)
    }

    fn signature_entries(
        &mut self,
        heads: &[ChangeHash],
    ) -> Result<Vec<(ObjId, ChangeHash, FacetSignature)>, ClaimHistoryError> {
        let key = frontier_key(heads);
        if let Some(entries) = self.signature_cache.get(&key) {
            return Ok(entries.clone());
        }
        let lists = self.signature_lists(heads)?;
        let total_length: usize = lists
            .iter()
            .map(|list| self.doc.length_at(list, heads))
            .sum();
        if total_length > self.budget.max_entries_per_frontier {
            return Err(ClaimHistoryError::ResourceRefused(
                "signature frontier budget exceeded",
            ));
        }
        let mut entries = Vec::with_capacity(total_length);
        for list in lists {
            for index in 0..self.doc.length_at(&list, heads) {
                let values = self
                    .doc
                    .get_all_at(&list, index, heads)
                    .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
                account_frontier_values(
                    &mut self.frontier_entries,
                    self.budget,
                    heads,
                    values.len(),
                )?;
                let values: Vec<_> = values
                    .into_iter()
                    .map(|(value, marker)| (value.into_owned(), marker))
                    .collect();
                if values.is_empty() {
                    return Err(ClaimHistoryError::MissingEvidence("signature list entry"));
                }
                for (value, marker) in values {
                    let hash = self.doc.hash_for_opid(&marker).ok_or(
                        ClaimHistoryError::MissingEvidence("signature entry operation hash"),
                    )?;
                    let object = match value {
                        Value::Object(ObjType::Map) => marker.clone(),
                        _ => {
                            return Err(ClaimHistoryError::MalformedWitness(
                                "signature entry is not a map",
                            ));
                        }
                    };
                    let signature = self.read_signature(&object, heads)?;
                    self.hydrated_signatures.insert(
                        SignatureKey {
                            hash,
                            marker: marker.clone(),
                        },
                        signature.clone(),
                    );
                    entries.push((marker, hash, signature));
                }
            }
        }
        // The claim budget also bounds the number of immutable frontiers retained. When it is
        // exhausted, skip insertion rather than converting a bounded cache into unbounded memory.
        if self.signature_cache.len() < self.budget.max_claims {
            self.signature_cache.insert(key, entries.clone());
        }
        Ok(entries)
    }

    fn signature_lists(&mut self, heads: &[ChangeHash]) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let mut lists = Vec::new();
        for meta in self.facet_metas(heads)? {
            let values = self
                .doc
                .get_all_at(&meta, "signatures", heads)
                .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
            account_frontier_values(&mut self.frontier_entries, self.budget, heads, values.len())?;
            let values: Vec<_> = values
                .into_iter()
                .map(|(value, marker)| (value.into_owned(), marker))
                .collect();
            for (value, object) in values {
                if !matches!(value, Value::Object(ObjType::List)) {
                    return Err(ClaimHistoryError::MalformedWitness(
                        "dmeta signatures is not a list",
                    ));
                }
                if !lists.contains(&object) {
                    lists.push(object);
                }
            }
        }
        if lists.is_empty() {
            Err(ClaimHistoryError::MissingEvidence("dmeta signatures list"))
        } else {
            Ok(lists)
        }
    }

    fn facet_metas(&mut self, heads: &[ChangeHash]) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let facets = self.map_children(&automerge::ROOT, "facets", heads, "facets map")?;
        let mut dmeta_facets = Vec::new();
        for facets_map in facets {
            dmeta_facets.extend(self.map_children(
                &facets_map,
                "org.example.daybook.dmeta/main",
                heads,
                "dmeta facet",
            )?);
        }
        let mut facet_maps = Vec::new();
        for dmeta in dmeta_facets {
            facet_maps.extend(self.map_children(&dmeta, "facets", heads, "dmeta facets map")?);
        }
        let mut metas = Vec::new();
        for facet_map in facet_maps {
            for meta in self.map_children(
                &facet_map,
                self.facet_key.to_string().as_str(),
                heads,
                "facet metadata",
            )? {
                if !metas.contains(&meta) {
                    metas.push(meta);
                }
            }
        }
        if metas.is_empty() {
            Err(ClaimHistoryError::MissingEvidence("facet metadata"))
        } else {
            Ok(metas)
        }
    }

    fn map_children(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
        missing: &'static str,
    ) -> Result<Vec<ObjId>, ClaimHistoryError> {
        let values = self
            .doc
            .get_all_at(object, property, heads)
            .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
        account_frontier_values(&mut self.frontier_entries, self.budget, heads, values.len())?;
        let values: Vec<_> = values
            .into_iter()
            .map(|(value, marker)| (value.into_owned(), marker))
            .collect();
        let mut children = Vec::new();
        for (value, child) in values {
            match value {
                Value::Object(ObjType::Map) if !children.contains(&child) => children.push(child),
                Value::Object(ObjType::Map) => {}
                _ => return Err(ClaimHistoryError::MalformedWitness(missing)),
            }
        }
        if children.is_empty() {
            Err(ClaimHistoryError::MissingEvidence(missing))
        } else {
            Ok(children)
        }
    }

    fn read_signature(
        &mut self,
        object: &ObjId,
        heads: &[ChangeHash],
    ) -> Result<FacetSignature, ClaimHistoryError> {
        Ok(FacetSignature {
            actor_public_key: self.read_bytes(object, "actorPublicKey", heads)?,
            signature: self.read_bytes(object, "signature", heads)?,
            origin_doc: None,
            inherited_message: None,
            inherited_value: None,
        })
    }

    fn read_bytes(
        &mut self,
        object: &ObjId,
        property: &str,
        heads: &[ChangeHash],
    ) -> Result<Vec<u8>, ClaimHistoryError> {
        let values = self
            .doc
            .get_all_at(object, property, heads)
            .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
        account_frontier_values(&mut self.frontier_entries, self.budget, heads, values.len())?;
        let values: Vec<_> = values
            .into_iter()
            .map(|(value, marker)| (value.into_owned(), marker))
            .collect();
        if values.is_empty() {
            return Err(ClaimHistoryError::MissingEvidence("signature bytes"));
        }
        let mut bytes = None;
        for (value, _) in values {
            let Value::Scalar(scalar) = value else {
                return Err(ClaimHistoryError::MalformedWitness(
                    "signature field is not bytes",
                ));
            };
            let ScalarValue::Bytes(candidate) = scalar.as_ref() else {
                return Err(ClaimHistoryError::MalformedWitness(
                    "signature field is not bytes",
                ));
            };
            if let Some(existing) = &bytes {
                if existing != candidate {
                    return Err(ClaimHistoryError::MissingEvidence(
                        "conflicting signature bytes",
                    ));
                }
            } else {
                bytes = Some(candidate.clone());
            }
        }
        bytes.ok_or(ClaimHistoryError::MissingEvidence("signature bytes"))
    }

    fn read_chosen_uuid(&mut self, heads: &[ChangeHash]) -> Result<Uuid, ClaimHistoryError> {
        let mut uuids = Vec::new();
        for meta in self.facet_metas(heads)? {
            for property in ["uuids", "uuid"] {
                let values = self
                    .doc
                    .get_all_at(&meta, property, heads)
                    .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
                account_frontier_values(
                    &mut self.frontier_entries,
                    self.budget,
                    heads,
                    values.len(),
                )?;
                let values: Vec<_> = values
                    .into_iter()
                    .map(|(value, marker)| (value.into_owned(), marker))
                    .collect();
                for (value, object) in values {
                    match value {
                        Value::Scalar(scalar) => uuids.push(parse_uuid(scalar.as_ref())?),
                        Value::Object(ObjType::List) => {
                            let length = self.doc.length_at(&object, heads);
                            if length > self.budget.max_entries_per_frontier {
                                return Err(ClaimHistoryError::ResourceRefused(
                                    "UUID frontier budget exceeded",
                                ));
                            }
                            for index in 0..length {
                                let entries = self.doc.get_all_at(&object, index, heads).map_err(
                                    |error| ClaimHistoryError::Automerge(error.to_string()),
                                )?;
                                account_frontier_values(
                                    &mut self.frontier_entries,
                                    self.budget,
                                    heads,
                                    entries.len(),
                                )?;
                                let entries: Vec<_> = entries
                                    .into_iter()
                                    .map(|(value, marker)| (value.into_owned(), marker))
                                    .collect();
                                if entries.is_empty() {
                                    return Err(ClaimHistoryError::MalformedWitness(
                                        "UUID list entry",
                                    ));
                                }
                                for (entry, _) in entries {
                                    let Value::Scalar(scalar) = entry else {
                                        return Err(ClaimHistoryError::MalformedWitness(
                                            "UUID list entry",
                                        ));
                                    };
                                    uuids.push(parse_uuid(scalar.as_ref())?);
                                }
                            }
                        }
                        _ => return Err(ClaimHistoryError::MalformedWitness("facet UUID shape")),
                    }
                }
            }
        }
        uuids
            .into_iter()
            .min()
            .ok_or(ClaimHistoryError::MissingEvidence("facet UUID alias"))
    }

    fn read_operation(
        &mut self,
        heads: &[ChangeHash],
    ) -> Result<ClaimOperation, ClaimHistoryError> {
        let mut saw_set = false;
        let mut saw_remove = false;
        let mut malformed = false;
        for meta in self.facet_metas(heads)? {
            let values = self
                .doc
                .get_all_at(&meta, "deletedAt", heads)
                .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
            account_frontier_values(&mut self.frontier_entries, self.budget, heads, values.len())?;
            let values: Vec<_> = values
                .into_iter()
                .map(|(value, marker)| (value.into_owned(), marker))
                .collect();
            if values.is_empty() {
                saw_set = true;
                continue;
            }
            for (value, object) in values {
                match value {
                    Value::Object(ObjType::List) if self.doc.length_at(&object, heads) > 0 => {
                        saw_remove = true;
                    }
                    Value::Object(ObjType::List) => {
                        saw_set = true;
                    }
                    _ => malformed = true,
                }
            }
        }
        if malformed {
            return Err(ClaimHistoryError::MalformedWitness("deletedAt shape"));
        }
        if saw_set && saw_remove {
            // Root ADR 015 leaves concurrent live/tombstone metadata as unresolved evidence;
            // do not invent a delete-wins policy or present either side as accepted.
            return Err(ClaimHistoryError::MissingEvidence(
                "conflicting live and tombstone operation evidence",
            ));
        }
        if saw_remove {
            Ok(ClaimOperation::Remove)
        } else {
            Ok(ClaimOperation::Set)
        }
    }

    fn read_facet_value(&mut self, heads: &[ChangeHash]) -> Result<JsonValue, ClaimHistoryError> {
        let facets = self.map_children(&automerge::ROOT, "facets", heads, "facets map")?;
        let property = self.facet_key.to_string();
        let mut values = Vec::new();
        for facets_map in facets {
            let facet_values = self
                .doc
                .get_all_at(&facets_map, property.as_str(), heads)
                .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
            account_frontier_values(
                &mut self.frontier_entries,
                self.budget,
                heads,
                facet_values.len(),
            )?;
            let facet_values: Vec<_> = facet_values
                .into_iter()
                .map(|(value, marker)| (value.into_owned(), marker))
                .collect();
            for (value, object) in facet_values {
                values.push(match value {
                    Value::Object(_) => self.read_json_object(&object, heads)?,
                    Value::Scalar(scalar) => scalar_to_json(scalar.as_ref())?,
                });
            }
        }
        let Some(first) = values.first().cloned() else {
            return Err(ClaimHistoryError::MissingEvidence("facet value"));
        };
        if values.iter().any(|value| value != &first) {
            return Err(ClaimHistoryError::MissingEvidence(
                "conflicting facet values",
            ));
        }
        Ok(first)
    }

    fn read_json_object(
        &mut self,
        object: &ObjId,
        heads: &[ChangeHash],
    ) -> Result<JsonValue, ClaimHistoryError> {
        self.value_nodes += 1;
        if self.value_nodes > self.budget.max_value_nodes {
            return Err(ClaimHistoryError::ResourceRefused(
                "facet value budget exceeded",
            ));
        }
        match self.doc.object_type(object) {
            Ok(ObjType::Map | ObjType::Table) => {
                let keys: Vec<String> = self
                    .doc
                    .map_range_at(object, .., heads)
                    .map(|item| item.key.to_string())
                    .collect();
                let mut result = Map::new();
                for key in keys {
                    let entries = self
                        .doc
                        .get_all_at(object, key.as_str(), heads)
                        .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
                    account_frontier_values(
                        &mut self.frontier_entries,
                        self.budget,
                        heads,
                        entries.len(),
                    )?;
                    let entries: Vec<_> = entries
                        .into_iter()
                        .map(|(value, marker)| (value.into_owned(), marker))
                        .collect();
                    if entries.is_empty() {
                        continue;
                    }
                    let mut decoded = None;
                    for (value, child) in entries {
                        let candidate = self.read_json_value(value, child, heads)?;
                        if let Some(existing) = &decoded {
                            if existing != &candidate {
                                return Err(ClaimHistoryError::MissingEvidence(
                                    "conflicting facet object values",
                                ));
                            }
                        } else {
                            decoded = Some(candidate);
                        }
                    }
                    result.insert(
                        key,
                        decoded.ok_or(ClaimHistoryError::MissingEvidence("facet map value"))?,
                    );
                }
                Ok(JsonValue::Object(result))
            }
            Ok(ObjType::List) => {
                let mut result = Vec::new();
                for index in 0..self.doc.length_at(object, heads) {
                    let entries = self
                        .doc
                        .get_all_at(object, index, heads)
                        .map_err(|error| ClaimHistoryError::Automerge(error.to_string()))?;
                    account_frontier_values(
                        &mut self.frontier_entries,
                        self.budget,
                        heads,
                        entries.len(),
                    )?;
                    let entries: Vec<_> = entries
                        .into_iter()
                        .map(|(value, marker)| (value.into_owned(), marker))
                        .collect();
                    if entries.is_empty() {
                        return Err(ClaimHistoryError::MissingEvidence("facet list value"));
                    }
                    let mut decoded = None;
                    for (value, child) in entries {
                        let candidate = self.read_json_value(value, child, heads)?;
                        if let Some(existing) = &decoded {
                            if existing != &candidate {
                                return Err(ClaimHistoryError::MissingEvidence(
                                    "conflicting facet list values",
                                ));
                            }
                        } else {
                            decoded = Some(candidate);
                        }
                    }
                    result.push(
                        decoded.ok_or(ClaimHistoryError::MissingEvidence("facet list value"))?,
                    );
                }
                Ok(JsonValue::Array(result))
            }
            Ok(ObjType::Text) => {
                Ok(JsonValue::String(self.doc.text_at(object, heads).map_err(
                    |error| ClaimHistoryError::Automerge(error.to_string()),
                )?))
            }
            Err(error) => Err(ClaimHistoryError::Automerge(error.to_string())),
        }
    }

    fn read_json_value(
        &mut self,
        value: Value<'_>,
        child: ObjId,
        heads: &[ChangeHash],
    ) -> Result<JsonValue, ClaimHistoryError> {
        match value {
            Value::Object(_) => self.read_json_object(&child, heads),
            Value::Scalar(scalar) => scalar_to_json(scalar.as_ref()),
        }
    }
}

fn parse_uuid(scalar: &ScalarValue) -> Result<Uuid, ClaimHistoryError> {
    match scalar {
        ScalarValue::Bytes(bytes) => Uuid::from_slice(bytes)
            .map_err(|_| ClaimHistoryError::MalformedWitness("invalid UUID bytes")),
        ScalarValue::Str(text) => Uuid::parse_str(text)
            .map_err(|_| ClaimHistoryError::MalformedWitness("invalid UUID text")),
        _ => Err(ClaimHistoryError::MalformedWitness(
            "UUID is not text or bytes",
        )),
    }
}

fn scalar_to_json(scalar: &ScalarValue) -> Result<JsonValue, ClaimHistoryError> {
    match scalar {
        ScalarValue::Null => Ok(JsonValue::Null),
        ScalarValue::Boolean(value) => Ok(JsonValue::Bool(*value)),
        ScalarValue::Str(value) => Ok(JsonValue::String(value.to_string())),
        ScalarValue::Int(value) => Ok(JsonValue::Number((*value).into())),
        ScalarValue::Uint(value) => Ok(JsonValue::Number((*value).into())),
        ScalarValue::F64(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or(ClaimHistoryError::MalformedWitness(
                "non-finite facet number",
            )),
        ScalarValue::Timestamp(value) => Ok(JsonValue::Number((*value).into())),
        ScalarValue::Bytes(_) => Err(ClaimHistoryError::MissingEvidence(
            "schema-aware bytes representation",
        )),
        ScalarValue::Counter(_) | ScalarValue::Unknown { .. } => Err(
            ClaimHistoryError::MissingEvidence("schema-aware native facet representation"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use automerge::{Automerge, ObjType, ReadDoc, ScalarValue};
    use big_cabinet_core::FacetKey;
    use big_cabinet_core::dmeta::{
        ClaimOperation, DocumentId, FacetClaim, SignedFacetClaim, value_cid,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::{Value as JsonValue, json};
    use uuid::Uuid;

    const FACET: &str = "org.example.note/main";

    fn fixture() -> (Automerge, DocumentId, FacetKey, Uuid, SigningKey) {
        let mut doc = Automerge::new();
        let document_id = DocumentId::new([7; 32]);
        let facet_key = FacetKey::from_text("org.example.note", "main").unwrap();
        let uuid = Uuid::from_bytes([0x80; 16]);
        let mut tx = doc.transaction();
        let facets = tx
            .put_object(automerge::ROOT, "facets", ObjType::Map)
            .unwrap();
        let dmeta = tx
            .put_object(&facets, "org.example.daybook.dmeta/main", ObjType::Map)
            .unwrap();
        let dmeta_facets = tx.put_object(&dmeta, "facets", ObjType::Map).unwrap();
        let facet_meta = tx.put_object(&dmeta_facets, FACET, ObjType::Map).unwrap();
        let uuids = tx.put_object(&facet_meta, "uuids", ObjType::List).unwrap();
        tx.insert(&uuids, 0, ScalarValue::Bytes(uuid.as_bytes().to_vec()))
            .unwrap();
        tx.put_object(&facet_meta, "signatures", ObjType::List)
            .unwrap();
        tx.put(&facets, FACET, "seed").unwrap();
        tx.commit().0.unwrap();
        (
            doc,
            document_id,
            facet_key,
            uuid,
            SigningKey::from_bytes(&[9; 32]),
        )
    }

    fn paths(doc: &Automerge) -> (ObjId, ObjId, ObjId, ObjId, ObjId, ObjId) {
        let (_, facets) = doc.get(automerge::ROOT, "facets").unwrap().unwrap();
        let (_, dmeta) = doc
            .get(&facets, "org.example.daybook.dmeta/main")
            .unwrap()
            .unwrap();
        let (_, dmeta_facets) = doc.get(&dmeta, "facets").unwrap().unwrap();
        let (_, meta) = doc.get(&dmeta_facets, FACET).unwrap().unwrap();
        let (_, signatures) = doc.get(&meta, "signatures").unwrap().unwrap();
        let (_, uuids) = doc.get(&meta, "uuids").unwrap().unwrap();
        (facets, dmeta, dmeta_facets, meta, signatures, uuids)
    }

    fn append_claim(
        doc: &mut Automerge,
        document_id: &DocumentId,
        facet_key: &FacetKey,
        uuid: Uuid,
        key: &SigningKey,
        operation: ClaimOperation,
        value: &JsonValue,
    ) -> (ObjId, ChangeHash) {
        let claim = FacetClaim {
            origin_document: document_id.clone(),
            facet_key: facet_key.clone(),
            chosen_uuid: uuid,
            operation,
            value_cid: match operation {
                ClaimOperation::Set => Some(value_cid(value).unwrap()),
                ClaimOperation::Remove => None,
            },
            predecessors: Vec::new(),
        };
        append_signed_claim(doc, key, value, claim)
    }

    fn append_signed_claim(
        doc: &mut Automerge,
        key: &SigningKey,
        value: &JsonValue,
        claim: FacetClaim,
    ) -> (ObjId, ChangeHash) {
        let signature = key
            .sign(&claim.signing_bytes().unwrap())
            .to_bytes()
            .to_vec();
        let (facets, _, _, meta, signatures, _) = paths(doc);
        let mut tx = doc.transaction();
        match claim.operation {
            ClaimOperation::Set => match value {
                JsonValue::Null => tx.put(&facets, FACET, ScalarValue::Null).unwrap(),
                JsonValue::String(text) => tx.put(&facets, FACET, text.as_str()).unwrap(),
                other => panic!("fixture value is not a scalar string/null: {other}"),
            },
            ClaimOperation::Remove => {
                let deleted_at = tx.put_object(&meta, "deletedAt", ObjType::List).unwrap();
                tx.insert(&deleted_at, 0, 1_i64).unwrap();
            }
        }
        // Dmeta signatures are a current competing set.  A sequential writer
        // replaces the entries it observed; their introducing changes remain
        // available for historical reconstruction.
        while tx.length(&signatures) > 0 {
            tx.delete(&signatures, 0).unwrap();
        }
        let marker = tx.insert_object(&signatures, 0, ObjType::Map).unwrap();
        tx.put(
            &marker,
            "actorPublicKey",
            key.verifying_key().to_bytes().to_vec(),
        )
        .unwrap();
        tx.put(&marker, "signature", signature).unwrap();
        let hash = tx.commit().0.unwrap();
        (marker, hash)
    }

    fn replace_signature_slot(
        doc: &mut Automerge,
        key: &SigningKey,
        signature: Vec<u8>,
    ) -> (ObjId, ChangeHash) {
        let (_, _, _, _, signatures, _) = paths(doc);
        let mut tx = doc.transaction();
        assert_eq!(tx.length(&signatures), 1);
        let marker = tx.put_object(&signatures, 0, ObjType::Map).unwrap();
        tx.put(
            &marker,
            "actorPublicKey",
            key.verifying_key().to_bytes().to_vec(),
        )
        .unwrap();
        tx.put(&marker, "signature", signature).unwrap();
        let hash = tx.commit().0.unwrap();
        (marker, hash)
    }

    fn append_invalid_signature_replacement(
        doc: &mut Automerge,
        key: &SigningKey,
    ) -> (ObjId, ChangeHash) {
        replace_signature_slot(doc, key, vec![0; 64])
    }

    #[test]
    fn introduction_exid_maps_to_its_change_and_uses_parent_frontier() {
        let (mut doc, document_id, facet_key, uuid, key) = fixture();
        let (marker, introduction_hash) = append_claim(
            &mut doc,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("first"),
        );
        let lower_uuid = Uuid::from_bytes([1; 16]);
        let (facets, _, _, _, _, uuids) = paths(&doc);
        let mut later = doc.transaction();
        later.put(&facets, FACET, "future-merged-value").unwrap();
        later
            .insert(
                &uuids,
                0,
                ScalarValue::Bytes(lower_uuid.as_bytes().to_vec()),
            )
            .unwrap();
        later.commit().0.unwrap();

        let result = reconstruct_claim_for_marker(
            &doc,
            &document_id,
            &facet_key,
            &marker,
            HistoryBudget::default(),
        );
        let ClaimHistoryResult::Accepted(reconstructed) = result else {
            panic!("{result:?}")
        };
        assert_eq!(reconstructed.introduction.marker, marker);
        assert_eq!(
            reconstructed.introduction.introduction_hash,
            introduction_hash
        );
        assert_eq!(reconstructed.claim.chosen_uuid, uuid);
        assert_eq!(
            reconstructed.claim.value_cid,
            Some(value_cid(&json!("first")).unwrap())
        );
        assert_eq!(reconstructed.claim.operation, ClaimOperation::Set);
        assert_eq!(reconstructed.introduction.parents.len(), 1);
        assert_eq!(reconstructed.introduction.resulting_heads, {
            let mut heads = reconstructed.introduction.parents.clone();
            heads.push(introduction_hash);
            heads
        });
    }

    #[test]
    fn set_null_is_not_remove_and_remove_has_no_value_cid() {
        let (mut doc, document_id, facet_key, uuid, key) = fixture();
        let (null_marker, _) = append_claim(
            &mut doc,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &JsonValue::Null,
        );
        let null_result = reconstruct_claim_for_marker(
            &doc,
            &document_id,
            &facet_key,
            &null_marker,
            HistoryBudget::default(),
        );
        let ClaimHistoryResult::Accepted(null_claim) = null_result else {
            panic!("{null_result:?}")
        };
        assert_eq!(null_claim.claim.operation, ClaimOperation::Set);
        assert_eq!(
            null_claim.claim.value_cid,
            Some(value_cid(&JsonValue::Null).unwrap())
        );

        let (mut remove_doc, remove_document_id, remove_facet_key, remove_uuid, remove_key) =
            fixture();
        let (remove_marker, _) = append_claim(
            &mut remove_doc,
            &remove_document_id,
            &remove_facet_key,
            remove_uuid,
            &remove_key,
            ClaimOperation::Remove,
            &JsonValue::Null,
        );
        let remove_result = reconstruct_claim_for_marker(
            &remove_doc,
            &remove_document_id,
            &remove_facet_key,
            &remove_marker,
            HistoryBudget::default(),
        );
        let ClaimHistoryResult::Accepted(remove_claim) = remove_result else {
            panic!("{remove_result:?}")
        };
        assert_eq!(remove_claim.claim.operation, ClaimOperation::Remove);
        assert_eq!(remove_claim.claim.value_cid, None);
    }

    #[test]
    fn invalid_signature_is_forgery_but_missing_signature_evidence_is_pending() {
        let (mut doc, document_id, facet_key, _, key) = fixture();
        let (facets, _, _, _, signatures, _) = paths(&doc);
        let mut tx = doc.transaction();
        tx.put(&facets, FACET, "invalid").unwrap();
        let signature_index = tx.length(&signatures);
        let invalid_marker = tx
            .insert_object(&signatures, signature_index, ObjType::Map)
            .unwrap();
        tx.put(
            &invalid_marker,
            "actorPublicKey",
            key.verifying_key().to_bytes().to_vec(),
        )
        .unwrap();
        tx.put(&invalid_marker, "signature", vec![0; 64]).unwrap();
        let invalid_hash = tx.commit().0.unwrap();
        let invalid_result = reconstruct_claim(
            &doc,
            &document_id,
            &facet_key,
            invalid_hash,
            HistoryBudget::default(),
        );
        assert!(matches!(
            invalid_result,
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
        ));

        let mut tx = doc.transaction();
        let signature_index = tx.length(&signatures);
        let missing_marker = tx
            .insert_object(&signatures, signature_index, ObjType::Map)
            .unwrap();
        tx.put(&missing_marker, "signature", vec![0; 64]).unwrap();
        let missing_hash = tx.commit().0.unwrap();
        let missing_result = reconstruct_claim(
            &doc,
            &document_id,
            &facet_key,
            missing_hash,
            HistoryBudget::default(),
        );
        assert!(matches!(
            missing_result,
            ClaimHistoryResult::Pending(ClaimHistoryError::MissingEvidence("signature bytes"))
        ));
    }

    #[test]
    fn history_and_frontier_budgets_refuse_without_becoming_forgery() {
        let (mut doc, document_id, facet_key, uuid, key) = fixture();
        let (marker, hash) = append_claim(
            &mut doc,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("bounded"),
        );
        let budget = HistoryBudget {
            max_claims: 0,
            ..HistoryBudget::default()
        };
        assert!(matches!(
            reconstruct_claim_for_marker(&doc, &document_id, &facet_key, &marker, budget),
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(
                "claim history budget exceeded"
            ))
        ));
        let budget = HistoryBudget {
            max_entries_per_frontier: 0,
            ..HistoryBudget::default()
        };
        assert!(matches!(
            reconstruct_claim(&doc, &document_id, &facet_key, hash, budget),
            ClaimHistoryResult::Pending(ClaimHistoryError::ResourceRefused(
                "signature frontier budget exceeded"
            ))
        ));
    }

    #[test]
    fn accepted_predecessors_are_reconstructed_not_all_visible_entries() {
        let (mut doc, document_id, facet_key, uuid, key) = fixture();
        let (first_marker, first_hash) = append_claim(
            &mut doc,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("one"),
        );
        let ClaimHistoryResult::Accepted(first) = reconstruct_claim_for_marker(
            &doc,
            &document_id,
            &facet_key,
            &first_marker,
            HistoryBudget::default(),
        ) else {
            panic!("first claim was not accepted")
        };
        assert_eq!(first.introduction.introduction_hash, first_hash);
        let first_signed = SignedFacetClaim::from_signature(
            first.claim,
            first.signature.actor_public_key,
            first.signature.signature,
        )
        .unwrap();
        let first_id = first_signed.signature_id().unwrap();

        // A sequential successor replaces the entry observed at its base.  The
        // first claim remains reconstructable from its introducing change, but
        // is no longer in the current competing set.
        let (successor_marker, successor_hash) = append_signed_claim(
            &mut doc,
            &key,
            &json!("two"),
            FacetClaim {
                value_cid: Some(value_cid(&json!("two")).unwrap()),
                predecessors: vec![first_id.clone()],
                ..first_signed.claim.clone()
            },
        );
        let ClaimHistoryResult::Accepted(successor) = reconstruct_claim_for_marker(
            &doc,
            &document_id,
            &facet_key,
            &successor_marker,
            HistoryBudget::default(),
        ) else {
            panic!("valid successor was not accepted")
        };
        assert_eq!(successor.claim.predecessors, vec![first_id]);
        let successor_signed = SignedFacetClaim::from_signature(
            successor.claim.clone(),
            successor.signature.actor_public_key.clone(),
            successor.signature.signature.clone(),
        )
        .unwrap();
        let successor_id = successor_signed.signature_id().unwrap();

        // Keep the clean multilevel chain separate from the concurrent invalid
        // replacement: the third claim's parent current set contains only the
        // successor, while the first claim remains historical evidence.
        let mut clean_chain = doc.fork();
        let mut invalid_branch = doc.fork();
        let (third_marker, third_hash) = append_signed_claim(
            &mut clean_chain,
            &key,
            &json!("three"),
            FacetClaim {
                value_cid: Some(value_cid(&json!("three")).unwrap()),
                predecessors: vec![successor_id.clone()],
                ..successor.claim.clone()
            },
        );
        let ClaimHistoryResult::Accepted(third) = reconstruct_claim_for_marker(
            &clean_chain,
            &document_id,
            &facet_key,
            &third_marker,
            HistoryBudget::default(),
        ) else {
            panic!("valid multi-level successor was not accepted")
        };
        assert_eq!(third.introduction.introduction_hash, third_hash);
        assert_eq!(third.introduction.parents, vec![successor_hash]);
        assert_eq!(third.claim.predecessors, vec![successor_id]);
        let third_signed = SignedFacetClaim::from_signature(
            third.claim.clone(),
            third.signature.actor_public_key.clone(),
            third.signature.signature.clone(),
        )
        .unwrap();
        let third_id = third_signed.signature_id().unwrap();

        // Make an invalid replacement concurrently with the valid third claim.
        // A later valid claim must exclude this visible-but-unaccepted entry
        // from its predecessor IDs.
        let (_invalid_marker, invalid_hash) =
            append_invalid_signature_replacement(&mut invalid_branch, &key);
        clean_chain.merge(&mut invalid_branch).unwrap();

        let (fourth_marker, fourth_hash) = append_signed_claim(
            &mut clean_chain,
            &key,
            &json!("four"),
            FacetClaim {
                value_cid: Some(value_cid(&json!("four")).unwrap()),
                predecessors: vec![third_id.clone()],
                ..third.claim.clone()
            },
        );
        let fourth_change = clean_chain.get_change_by_hash(&fourth_hash).unwrap();
        assert!(fourth_change.deps().contains(&invalid_hash));
        assert!(matches!(
            reconstruct_claim(
                &clean_chain,
                &document_id,
                &facet_key,
                invalid_hash,
                HistoryBudget::default(),
            ),
            ClaimHistoryResult::ProvenForgery(ClaimHistoryError::InvalidSignature)
        ));
        let ClaimHistoryResult::Accepted(fourth) = reconstruct_claim_for_marker(
            &clean_chain,
            &document_id,
            &facet_key,
            &fourth_marker,
            HistoryBudget::default(),
        ) else {
            panic!("valid claim after invalid predecessor was not accepted")
        };
        assert_eq!(fourth.claim.predecessors, vec![third_id]);

        // The replaced first entry is still recoverable from physical history.
        let ClaimHistoryResult::Accepted(recovered_first) = reconstruct_claim_for_marker(
            &clean_chain,
            &document_id,
            &facet_key,
            &first_marker,
            HistoryBudget::default(),
        ) else {
            panic!("replaced predecessor was not historically reconstructable")
        };
        assert_eq!(recovered_first.introduction.introduction_hash, first_hash);
        assert_eq!(
            recovered_first.claim.value_cid,
            Some(value_cid(&json!("one")).unwrap())
        );

        // The fourth reconstruction hydrates its parent frontier once.  A
        // second lookup must hit the canonical frontier cache without adding
        // another accounting entry.
        let mut state = Reconstruction {
            doc: &clean_chain,
            origin_document: &document_id,
            facet_key: &facet_key,
            budget: HistoryBudget::default(),
            claims_seen: 0,
            value_nodes: 0,
            cache: HashMap::new(),
            signature_cache: HashMap::new(),
            hydrated_signatures: HashMap::new(),
            frontier_entries: HashMap::new(),
            active: HashSet::new(),
        };
        let fourth_from_state = state.claim_for_entry(fourth_hash, fourth_marker).unwrap();
        let parent_frontier = frontier_key(&fourth_from_state.introduction.parents);
        assert!(state.signature_cache.contains_key(&parent_frontier));
        let cache_len = state.signature_cache.len();
        let accounting_before = state.frontier_entries.clone();
        state
            .signature_entries(&fourth_from_state.introduction.parents)
            .unwrap();
        assert_eq!(state.signature_cache.len(), cache_len);
        assert_eq!(state.frontier_entries, accounting_before);
    }

    fn operation_evidence_with_malformed_root(tombstone_first: bool) -> ClaimHistoryResult {
        let (base, document_id, facet_key, uuid, key) = fixture();
        let mut tombstone = base.fork();
        let mut malformed = base.fork();
        let (_, _, _, meta, _, _) = paths(&tombstone);
        let mut tx = tombstone.transaction();
        let deleted_at = tx.put_object(&meta, "deletedAt", ObjType::List).unwrap();
        tx.insert(&deleted_at, 0, 1_i64).unwrap();
        tx.commit().0.unwrap();
        let (_, _, _, malformed_meta, _, _) = paths(&malformed);
        let mut tx = malformed.transaction();
        tx.put(&malformed_meta, "deletedAt", "not-a-list").unwrap();
        tx.commit().0.unwrap();
        if tombstone_first {
            tombstone.merge(&mut malformed).unwrap();
        } else {
            malformed.merge(&mut tombstone).unwrap();
            tombstone = malformed;
        }
        let (marker, _) = append_claim(
            &mut tombstone,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("value"),
        );
        reconstruct_claim_for_marker(
            &tombstone,
            &document_id,
            &facet_key,
            &marker,
            HistoryBudget::default(),
        )
    }

    #[test]
    fn operation_scan_inspects_malformed_roots_after_tombstones() {
        for tombstone_first in [true, false] {
            assert!(matches!(
                operation_evidence_with_malformed_root(tombstone_first),
                ClaimHistoryResult::ProvenForgery(ClaimHistoryError::MalformedWitness(
                    "deletedAt shape"
                ))
            ));
        }
    }

    #[test]
    fn mixed_live_and_tombstone_operation_evidence_stays_pending() {
        let (base, document_id, facet_key, uuid, key) = fixture();
        let mut tombstone = base.fork();
        let mut live = base.fork();
        let (_, _, _, meta, _, _) = paths(&tombstone);
        let mut tx = tombstone.transaction();
        let deleted_at = tx.put_object(&meta, "deletedAt", ObjType::List).unwrap();
        tx.insert(&deleted_at, 0, 1_i64).unwrap();
        tx.commit().0.unwrap();
        let (_, _, _, live_meta, _, _) = paths(&live);
        let mut tx = live.transaction();
        tx.put_object(&live_meta, "deletedAt", ObjType::List)
            .unwrap();
        tx.commit().0.unwrap();
        tombstone.merge(&mut live).unwrap();
        let (marker, _) = append_claim(
            &mut tombstone,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("value"),
        );
        assert!(matches!(
            reconstruct_claim_for_marker(
                &tombstone,
                &document_id,
                &facet_key,
                &marker,
                HistoryBudget::default(),
            ),
            ClaimHistoryResult::Pending(ClaimHistoryError::MissingEvidence(
                "conflicting live and tombstone operation evidence"
            ))
        ));
    }

    #[test]
    fn conflict_fanout_at_one_list_position_is_budgeted_before_expansion() {
        let (mut base, document_id, facet_key, uuid, key) = fixture();
        append_claim(
            &mut base,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("base"),
        );
        let mut left = base.fork();
        let mut right = base.fork();
        replace_signature_slot(&mut left, &key, vec![1; 64]);
        replace_signature_slot(&mut right, &key, vec![2; 64]);
        left.merge(&mut right).unwrap();

        let heads = left.get_heads();
        let (_, _, _, _, signatures, _) = paths(&left);
        assert_eq!(left.length_at(&signatures, &heads), 1);
        let values = left.get_all_at(&signatures, 0, &heads).unwrap();
        assert!(values.len() >= 2);

        // This direct scoped assertion proves the returned multiplicity, not
        // the logical list length, is what exceeds the frontier budget.
        let mut frontier_entries = HashMap::new();
        assert!(matches!(
            account_frontier_values(
                &mut frontier_entries,
                HistoryBudget {
                    max_entries_per_frontier: 1,
                    ..HistoryBudget::default()
                },
                &heads,
                values.len(),
            ),
            Err(ClaimHistoryError::ResourceRefused(
                "signature frontier budget exceeded"
            ))
        ));

        // The fixture has five one-value metadata/list-root reads before the
        // signature position.  Six permits those reads; the two-or-more
        // values returned for the single position must then refuse before
        // owned-value conversion or signature-cache insertion.
        let result = reconstruct_current_claims(
            &left,
            &document_id,
            &facet_key,
            &heads,
            HistoryBudget {
                max_entries_per_frontier: 6,
                ..HistoryBudget::default()
            },
        );
        assert!(matches!(
            result,
            Err(ClaimHistoryError::ResourceRefused(
                "signature frontier budget exceeded"
            ))
        ));
    }

    #[test]
    fn distinct_markers_in_one_change_do_not_alias_by_change_hash() {
        let (mut doc, document_id, facet_key, uuid, key) = fixture();
        let claim = FacetClaim {
            origin_document: document_id.clone(),
            facet_key: facet_key.clone(),
            chosen_uuid: uuid,
            operation: ClaimOperation::Set,
            value_cid: Some(value_cid(&json!("same")).unwrap()),
            predecessors: Vec::new(),
        };
        let signature = key
            .sign(&claim.signing_bytes().unwrap())
            .to_bytes()
            .to_vec();
        let (facets, _, _, _, signatures, _) = paths(&doc);
        let mut tx = doc.transaction();
        tx.put(&facets, FACET, "same").unwrap();
        for _ in 0..2 {
            let signature_index = tx.length(&signatures);
            let marker = tx
                .insert_object(&signatures, signature_index, ObjType::Map)
                .unwrap();
            tx.put(
                &marker,
                "actorPublicKey",
                key.verifying_key().to_bytes().to_vec(),
            )
            .unwrap();
            tx.put(&marker, "signature", signature.clone()).unwrap();
        }
        let hash = tx.commit().0.unwrap();
        let claims = reconstruct_current_claims(
            &doc,
            &document_id,
            &facet_key,
            &doc.get_heads(),
            HistoryBudget::default(),
        )
        .unwrap();
        assert_eq!(claims.len(), 2);
        assert!(
            claims
                .iter()
                .all(|claim| claim.introduction.introduction_hash == hash)
        );
        let by_marker: HashSet<_> = claims
            .iter()
            .map(|claim| claim.introduction.marker.clone())
            .collect();
        assert_eq!(by_marker.len(), 2);
    }

    #[test]
    fn concurrent_writers_remain_a_competing_current_set() {
        let (mut left, document_id, facet_key, uuid, key) = fixture();
        let mut right = left.fork();
        let (left_marker, _) = append_claim(
            &mut left,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("left"),
        );
        let (right_marker, _) = append_claim(
            &mut right,
            &document_id,
            &facet_key,
            uuid,
            &key,
            ClaimOperation::Set,
            &json!("right"),
        );
        left.merge(&mut right).unwrap();
        let heads = left.get_heads();
        let claims = reconstruct_current_claims(
            &left,
            &document_id,
            &facet_key,
            &heads,
            HistoryBudget::default(),
        )
        .unwrap();
        assert_eq!(claims.len(), 2);
        assert!(
            claims
                .iter()
                .any(|claim| claim.introduction.marker == left_marker)
        );
        assert!(
            claims
                .iter()
                .any(|claim| claim.introduction.marker == right_marker)
        );
        assert_ne!(claims[0].claim.value_cid, claims[1].claim.value_cid);
    }
}
