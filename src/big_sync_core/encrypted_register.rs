//! Policy-neutral signed latest-per-writer evidence for custom sync backends.
//!
//! Partition admission and domain eligibility are host responsibilities. Signatures
//! authenticate writer assertions, not complete causal knowledge or historical authority.
//! Ciphertext and key-reference metadata are opaque; local key availability never ranks state.

use std::collections::BTreeMap;

use ed25519_dalek::{Signature, Signer, VerifyingKey};
use serde::{Deserialize, Serialize};

const SCHEMA: u8 = 1;
const KEY_PREFIX: &[u8] = b"big-sync/register/v1\0";

/// Fixed-size Ed25519 signature, split into its two wire components for serde.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SignatureBytes {
    pub r: [u8; 32],
    pub s: [u8; 32],
}

impl SignatureBytes {
    const EMPTY: Self = Self {
        r: [0; 32],
        s: [0; 32],
    };

    fn sign<S: Signer<Signature> + ?Sized>(
        bytes: &[u8],
        signer: &S,
    ) -> Result<Self, RegisterError> {
        let signature = signer
            .try_sign(bytes)
            .map_err(|_| RegisterError::Signing)?
            .to_bytes();
        Ok(Self {
            r: signature[..32].try_into().expect("Ed25519 R component"),
            s: signature[32..].try_into().expect("Ed25519 S component"),
        })
    }

    fn append_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.r);
        bytes.extend_from_slice(&self.s);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RegisterKey {
    pub scope: Vec<u8>,
    pub slot: Vec<u8>,
}

impl RegisterKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(KEY_PREFIX.len() + 8 + self.scope.len() + self.slot.len());
        self.append_to(&mut bytes);
        bytes
    }

    fn append_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(KEY_PREFIX);
        append_bytes(bytes, &self.scope);
        append_bytes(bytes, &self.slot);
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RegisterError> {
        let mut remaining = bytes
            .strip_prefix(KEY_PREFIX)
            .ok_or(RegisterError::InvalidKey)?;
        fn component<'a>(remaining: &mut &'a [u8]) -> Result<&'a [u8], RegisterError> {
            let prefix = remaining.get(..4).ok_or(RegisterError::InvalidKey)?;
            let length = u32::from_be_bytes(prefix.try_into().expect("four-byte length")) as usize;
            *remaining = &remaining[4..];
            let value = remaining.get(..length).ok_or(RegisterError::InvalidKey)?;
            *remaining = &remaining[length..];
            Ok(value)
        }
        let scope = component(&mut remaining)?;
        let slot = component(&mut remaining)?;
        if !remaining.is_empty() {
            return Err(RegisterError::InvalidKey);
        }
        Ok(Self {
            scope: scope.to_vec(),
            slot: slot.to_vec(),
        })
    }
}

fn append_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .expect("bounded register field")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value);
}

/// One current causal dependency, not a historical event/digest chain.
/// References logical records and writer versions; never creates a physical key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CausalVersion {
    pub record: RegisterKey,
    pub writer: [u8; 32],
    pub writer_seq: u64,
}

fn append_frontier(bytes: &mut Vec<u8>, frontier: &[CausalVersion]) {
    bytes.extend_from_slice(&(frontier.len() as u64).to_be_bytes());
    for dependency in frontier {
        dependency.record.append_to(bytes);
        bytes.extend_from_slice(&dependency.writer);
        bytes.extend_from_slice(&dependency.writer_seq.to_be_bytes());
    }
}

fn valid_frontier(
    frontier: &[CausalVersion],
    key: &RegisterKey,
    writer: [u8; 32],
    sequence: u64,
) -> bool {
    frontier
        .windows(2)
        .all(|pair| (&pair[0].record, pair[0].writer) < (&pair[1].record, pair[1].writer))
        && frontier.iter().all(|entry| {
            entry.record != *key || entry.writer != writer || entry.writer_seq < sequence
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneHeader {
    pub schema: u8,
    pub key: RegisterKey,
    pub writer: [u8; 32],
    pub writer_seq: u64,
    pub frontier: Vec<CausalVersion>,
    /// Hash of complete ORIGINAL signed statement, not a naked payload digest.
    pub commitment: [u8; 32],
    pub signature: SignatureBytes,
}

impl LaneHeader {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"big-sync/register-lane/v1\0".to_vec();
        bytes.push(self.schema);
        self.key.append_to(&mut bytes);
        bytes.extend_from_slice(&self.writer);
        bytes.extend_from_slice(&self.writer_seq.to_be_bytes());
        append_frontier(&mut bytes, &self.frontier);
        bytes.extend_from_slice(&self.commitment);
        bytes
    }

    /// Includes public causal metadata as well as the original signed-statement
    /// commitment. A writer cannot reuse one hidden commitment to equivocate
    /// about its frontier without changing this public semantic identity.
    pub fn semantic_identity(&self) -> [u8; 32] {
        *blake3::hash(&self.signing_bytes()).as_bytes()
    }

    fn same_semantics(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.key == other.key
            && self.writer == other.writer
            && self.writer_seq == other.writer_seq
            && self.frontier == other.frontier
            && self.commitment == other.commitment
    }

    pub fn verify(&self) -> Result<(), RegisterError> {
        if self.schema != SCHEMA {
            return Err(RegisterError::UnsupportedSchema);
        }
        if !valid_frontier(&self.frontier, &self.key, self.writer, self.writer_seq) {
            return Err(RegisterError::Frontier);
        }
        verify(self.writer, &self.signing_bytes(), &self.signature)
    }

    /// Direct compact frontier dominance. Drivers resolve dependencies before
    /// domain effects; this reducer does not walk arbitrary cross-record DAGs.
    pub fn observes(&self, other: &LaneHeader) -> bool {
        self.frontier.iter().any(|entry| {
            entry.record == other.key
                && entry.writer == other.writer
                && entry.writer_seq >= other.writer_seq
        })
    }
}

/// Encrypted by the driver; never placed in a public BigSync payload directly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginalStatement {
    pub key: RegisterKey,
    pub writer: [u8; 32],
    pub writer_seq: u64,
    pub frontier: Vec<CausalVersion>,
    pub body: Vec<u8>,
    pub signature: SignatureBytes,
}

impl OriginalStatement {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"big-sync/register-statement/v1\0".to_vec();
        self.key.append_to(&mut bytes);
        bytes.extend_from_slice(&self.writer);
        bytes.extend_from_slice(&self.writer_seq.to_be_bytes());
        append_frontier(&mut bytes, &self.frontier);
        bytes.extend_from_slice(&(self.body.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&self.body);
        bytes
    }

    /// Returns neither statement nor header if either exact signing step fails.
    pub fn sign<S: Signer<Signature> + ?Sized>(
        key: RegisterKey,
        writer_seq: u64,
        frontier: Vec<CausalVersion>,
        body: Vec<u8>,
        verifying_key: VerifyingKey,
        signer: &S,
    ) -> Result<(Self, LaneHeader), RegisterError> {
        let writer = verifying_key.to_bytes();
        assert!(
            valid_frontier(&frontier, &key, writer, writer_seq),
            "local producer supplies canonical causal frontier"
        );
        let mut statement = Self {
            key,
            writer,
            writer_seq,
            frontier,
            body,
            signature: SignatureBytes::EMPTY,
        };
        statement.signature = SignatureBytes::sign(&statement.signing_bytes(), signer)?;
        let mut header = LaneHeader {
            schema: SCHEMA,
            key: statement.key.clone(),
            writer: statement.writer,
            writer_seq,
            frontier: statement.frontier.clone(),
            commitment: statement.commitment(),
            signature: SignatureBytes::EMPTY,
        };
        header.signature = SignatureBytes::sign(&header.signing_bytes(), signer)?;
        header.verify()?;
        Ok((statement, header))
    }

    fn commitment(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"big-sync/register-signed-statement/v1\0");
        hash.update(&self.signing_bytes());
        hash.update(&self.signature.r);
        hash.update(&self.signature.s);
        *hash.finalize().as_bytes()
    }

    /// Reader calls this after decrypting the complete original statement.
    pub fn verify_header(&self, header: &LaneHeader) -> Result<(), RegisterError> {
        header.verify()?;
        verify(self.writer, &self.signing_bytes(), &self.signature)?;
        if self.key != header.key
            || self.writer != header.writer
            || self.writer_seq != header.writer_seq
            || self.frontier != header.frontier
            || self.commitment() != header.commitment
        {
            return Err(RegisterError::Binding);
        }
        Ok(())
    }
}

/// A rewrapper signs only its ciphertext representation, never the writer lane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Representation {
    pub original: LaneHeader,
    pub binding: RepresentationBinding,
    pub publisher: [u8; 32],
    pub ciphertext: Vec<u8>,
    pub signature: SignatureBytes,
}

impl Representation {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"big-sync/register-representation/v1\0".to_vec();
        bytes.extend_from_slice(&self.original.signing_bytes());
        self.original.signature.append_to(&mut bytes);
        self.binding.append_to(&mut bytes);
        bytes.extend_from_slice(&self.publisher);
        bytes.extend_from_slice(&(self.ciphertext.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&self.ciphertext);
        bytes
    }

    /// Returns no representation when the exact signing step fails.
    pub fn sign<S: Signer<Signature> + ?Sized>(
        original: LaneHeader,
        binding: RepresentationBinding,
        ciphertext: Vec<u8>,
        verifying_key: VerifyingKey,
        signer: &S,
    ) -> Result<Self, RegisterError> {
        let mut value = Self {
            original,
            binding,
            publisher: verifying_key.to_bytes(),
            ciphertext,
            signature: SignatureBytes::EMPTY,
        };
        value.signature = SignatureBytes::sign(&value.signing_bytes(), signer)?;
        value.verify()?;
        Ok(value)
    }

    fn verify(&self) -> Result<(), RegisterError> {
        self.original.verify()?;
        if !self
            .binding
            .key_heads
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(RegisterError::Binding);
        }
        verify(self.publisher, &self.signing_bytes(), &self.signature)
    }

    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.binding
            .cmp(&other.binding)
            .then_with(|| self.publisher.cmp(&other.publisher))
            .then_with(|| self.ciphertext.cmp(&other.ciphertext))
            .then_with(|| self.original.signature.cmp(&other.original.signature))
            .then_with(|| self.signature.cmp(&other.signature))
    }
}

fn verify(key: [u8; 32], bytes: &[u8], signature: &SignatureBytes) -> Result<(), RegisterError> {
    let key = VerifyingKey::from_bytes(&key).map_err(|_| RegisterError::Signature)?;
    let signature = Signature::from_components(signature.r, signature.s);
    key.verify_strict(bytes, &signature)
        .map_err(|_| RegisterError::Signature)
}

/// Opaque representation metadata authenticated by its publisher. Hosts resolve
/// references and interpret encoding parameters; the register does neither.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RepresentationBinding {
    pub incarnation: [u8; 32],
    pub key_ref: Vec<u8>,
    pub key_heads: Vec<[u8; 32]>,
    pub encoding: Vec<u8>,
    pub parameters: Vec<u8>,
}

impl RepresentationBinding {
    fn append_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.incarnation);
        append_bytes(bytes, &self.key_ref);
        bytes.extend_from_slice(&(self.key_heads.len() as u64).to_be_bytes());
        for head in &self.key_heads {
            bytes.extend_from_slice(head);
        }
        append_bytes(bytes, &self.encoding);
        append_bytes(bytes, &self.parameters);
    }
}

/// Fixed structural budgets, not membership-dependent semantic pruning. A host
/// bounds decoding/transport separately; historical writer retirement is not provided here.
#[derive(Clone, Debug)]
pub struct Limits {
    pub key: RegisterKey,
    pub max_key_bytes: usize,
    pub max_ciphertext_bytes: usize,
    pub max_frontier_entries: usize,
    pub max_metadata_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaneState {
    /// One canonically selected representation, independent of local key availability.
    Current { representation: Representation },
    /// Exactly two least distinct semantic commitments. Invalid for scheduling.
    Equivocated { witnesses: [LaneHeader; 2] },
}

impl LaneState {
    pub fn writer_seq(&self) -> u64 {
        match self {
            Self::Current { representation } => representation.original.writer_seq,
            Self::Equivocated { witnesses } => witnesses[0].writer_seq,
        }
    }
}

/// Record-bound current state. Lanes encode as writer/value pairs, so binary
/// writer identities remain exact even in JSON payloads.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterSnapshot {
    pub schema: u8,
    pub key: RegisterKey,
    #[serde(
        serialize_with = "serialize_lanes",
        deserialize_with = "deserialize_lanes"
    )]
    pub lanes: BTreeMap<[u8; 32], LaneState>,
}

fn serialize_lanes<S: serde::Serializer>(
    lanes: &BTreeMap<[u8; 32], LaneState>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(lanes.iter())
}

fn deserialize_lanes<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<[u8; 32], LaneState>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = BTreeMap<[u8; 32], LaneState>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("unique writer/lane pairs")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut lanes = BTreeMap::new();
            while let Some((writer, lane)) = sequence.next_element()? {
                if lanes.insert(writer, lane).is_some() {
                    return Err(serde::de::Error::custom("duplicate register writer"));
                }
            }
            Ok(lanes)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    Unchanged,
    Changed,
    /// Mutation intentionally retains bounded invalidity/evidence, not atomic rejection.
    EquivocationRetained,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegisterError {
    #[error("invalid register key framing")]
    InvalidKey,
    #[error("unsupported register schema")]
    UnsupportedSchema,
    #[error("register signing failed")]
    Signing,
    #[error("invalid register signature")]
    Signature,
    #[error("register identity binding mismatch")]
    Binding,
    #[error("register key or representation metadata exceeds configured budget")]
    MetadataTooLarge,
    #[error("current representation exceeds configured body budget")]
    BodyTooLarge,
    #[error("noncanonical or self-future causal frontier")]
    Frontier,
    #[error("causal frontier exceeds configured current-dependency budget")]
    FrontierTooLarge,
    #[error("invalid or noncanonical equivocation witnesses")]
    Witnesses,
}

#[derive(Clone, Debug)]
pub struct EncryptedRegister {
    limits: Limits,
    lanes: BTreeMap<[u8; 32], LaneState>,
}

impl EncryptedRegister {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            lanes: BTreeMap::new(),
        }
    }
    pub fn lanes(&self) -> &BTreeMap<[u8; 32], LaneState> {
        &self.lanes
    }

    /// Serialize current evidence without copying ciphertext or the writer map.
    pub fn snapshot(&self) -> impl Serialize + '_ {
        #[derive(Serialize)]
        struct BorrowedSnapshot<'a> {
            schema: u8,
            key: &'a RegisterKey,
            #[serde(serialize_with = "serialize_lanes")]
            lanes: &'a BTreeMap<[u8; 32], LaneState>,
        }
        BorrowedSnapshot {
            schema: SCHEMA,
            key: &self.limits.key,
            lanes: &self.lanes,
        }
    }

    /// Restore and remote merge verify the same evidence; neither proves authority.
    pub fn restore(limits: Limits, snapshot: RegisterSnapshot) -> Result<Self, RegisterError> {
        let mut state = Self::new(limits);
        state.merge_snapshot(snapshot)?;
        Ok(state)
    }

    /// Authenticate the complete remote map before changing any local lane.
    /// Omitted lanes are not removals; poison is transferable signed evidence.
    pub fn merge_snapshot(
        &mut self,
        snapshot: RegisterSnapshot,
    ) -> Result<MergeOutcome, RegisterError> {
        self.validate_snapshot_binding(&snapshot)?;
        for (writer, lane) in &snapshot.lanes {
            self.validate_lane(*writer, lane)?;
        }
        for lane in snapshot.lanes.values() {
            Self::verify_lane(lane)?;
        }
        let mut outcome = MergeOutcome::Unchanged;
        for (writer, lane) in snapshot.lanes {
            match self.join_lane(writer, lane) {
                MergeOutcome::EquivocationRetained => outcome = MergeOutcome::EquivocationRetained,
                MergeOutcome::Changed if outcome == MergeOutcome::Unchanged => {
                    outcome = MergeOutcome::Changed;
                }
                _ => {}
            }
        }
        Ok(outcome)
    }

    fn validate_snapshot_binding(&self, snapshot: &RegisterSnapshot) -> Result<(), RegisterError> {
        if snapshot.schema != SCHEMA {
            return Err(RegisterError::UnsupportedSchema);
        }
        if snapshot.key != self.limits.key {
            return Err(RegisterError::Binding);
        }
        Ok(())
    }

    fn validate_header(&self, writer: [u8; 32], header: &LaneHeader) -> Result<(), RegisterError> {
        if header.schema != SCHEMA {
            return Err(RegisterError::UnsupportedSchema);
        }
        if header.key != self.limits.key || header.writer != writer {
            return Err(RegisterError::Binding);
        }
        if header.key.scope.len() > self.limits.max_key_bytes
            || header.key.slot.len() > self.limits.max_key_bytes
            || header.frontier.iter().any(|entry| {
                entry.record.scope.len() > self.limits.max_key_bytes
                    || entry.record.slot.len() > self.limits.max_key_bytes
            })
        {
            return Err(RegisterError::MetadataTooLarge);
        }
        if header.frontier.len() > self.limits.max_frontier_entries {
            return Err(RegisterError::FrontierTooLarge);
        }
        Ok(())
    }
    fn validate_representation(
        &self,
        representation: &Representation,
    ) -> Result<(), RegisterError> {
        self.validate_header(representation.original.writer, &representation.original)?;
        if representation.ciphertext.len() > self.limits.max_ciphertext_bytes {
            return Err(RegisterError::BodyTooLarge);
        }
        let binding = &representation.binding;
        let metadata_bytes = binding
            .key_ref
            .len()
            .checked_add(binding.encoding.len())
            .and_then(|size| size.checked_add(binding.parameters.len()))
            .and_then(|size| {
                binding
                    .key_heads
                    .len()
                    .checked_mul(32)
                    .and_then(|heads| size.checked_add(heads))
            })
            .ok_or(RegisterError::MetadataTooLarge)?;
        if metadata_bytes > self.limits.max_metadata_bytes {
            return Err(RegisterError::MetadataTooLarge);
        }
        Ok(())
    }

    /// Verify signed evidence and fixed budgets without copying its ciphertext.
    pub fn verify_representation(
        &self,
        representation: &Representation,
    ) -> Result<(), RegisterError> {
        self.validate_representation(representation)?;
        representation.verify()
    }

    fn validate_lane(&self, writer: [u8; 32], lane: &LaneState) -> Result<(), RegisterError> {
        match lane {
            LaneState::Current { representation } => {
                if writer != representation.original.writer {
                    return Err(RegisterError::Binding);
                }
                self.validate_representation(representation)?;
            }
            LaneState::Equivocated { witnesses } => {
                for witness in witnesses {
                    self.validate_header(writer, witness)?;
                }
                if witnesses[0].writer_seq != witnesses[1].writer_seq
                    || witnesses[0].same_semantics(&witnesses[1])
                {
                    return Err(RegisterError::Witnesses);
                }
            }
        }
        Ok(())
    }

    fn verify_lane(lane: &LaneState) -> Result<(), RegisterError> {
        match lane {
            LaneState::Current { representation } => representation.verify(),
            LaneState::Equivocated { witnesses } => {
                for witness in witnesses {
                    witness.verify()?;
                }
                if witnesses[0].semantic_identity() >= witnesses[1].semantic_identity() {
                    return Err(RegisterError::Witnesses);
                }
                Ok(())
            }
        }
    }

    /// Same-record desired heads visible without decrypting. Retain dominated
    /// writer lanes as replay fences; do not turn this projection into deletion.
    pub fn heads(&self) -> impl Iterator<Item = &LaneHeader> {
        self.lanes
            .values()
            .filter_map(|lane| match lane {
                LaneState::Current { representation } => Some(&representation.original),
                LaneState::Equivocated { .. } => None,
            })
            .filter(|candidate| {
                !self.lanes.values().any(|lane| match lane {
                    LaneState::Current { representation } => {
                        let original = &representation.original;
                        original.writer != candidate.writer && original.observes(candidate)
                    }
                    LaneState::Equivocated { .. } => false,
                })
            })
    }

    pub fn representation(&self, writer: &[u8; 32]) -> Option<&Representation> {
        match self.lanes.get(writer)? {
            LaneState::Current { representation } => Some(representation),
            LaneState::Equivocated { .. } => None,
        }
    }

    pub fn merge(&mut self, incoming: Representation) -> Result<MergeOutcome, RegisterError> {
        let writer = incoming.original.writer;
        let lane = LaneState::Current {
            representation: incoming,
        };
        self.validate_lane(writer, &lane)?;
        Self::verify_lane(&lane)?;
        Ok(self.join_lane(writer, lane))
    }

    fn join_lane(&mut self, writer: [u8; 32], incoming: LaneState) -> MergeOutcome {
        let Some(existing) = self.lanes.get_mut(&writer) else {
            let outcome = if matches!(incoming, LaneState::Equivocated { .. }) {
                MergeOutcome::EquivocationRetained
            } else {
                MergeOutcome::Changed
            };
            self.lanes.insert(writer, incoming);
            return outcome;
        };
        if incoming.writer_seq() < existing.writer_seq() {
            return MergeOutcome::Unchanged;
        }
        if incoming.writer_seq() > existing.writer_seq() {
            let outcome = if matches!(incoming, LaneState::Equivocated { .. }) {
                MergeOutcome::EquivocationRetained
            } else {
                MergeOutcome::Changed
            };
            *existing = incoming;
            return outcome;
        }
        if let (
            LaneState::Current {
                representation: retained,
            },
            LaneState::Current {
                representation: candidate,
            },
        ) = (&mut *existing, &incoming)
            && retained.original.same_semantics(&candidate.original)
        {
            if candidate.canonical_cmp(retained).is_lt() {
                *existing = incoming;
                return MergeOutcome::Changed;
            }
            return MergeOutcome::Unchanged;
        }
        // At most four headers participate. Keep exactly the two least distinct
        // semantic identities, including canonical signature variants.
        fn headers(lane: &LaneState) -> &[LaneHeader] {
            match lane {
                LaneState::Current { representation } => {
                    std::slice::from_ref(&representation.original)
                }
                LaneState::Equivocated { witnesses } => witnesses,
            }
        }
        let (first, second) = {
            let mut candidates = [None; 4];
            for (index, header) in headers(existing)
                .iter()
                .chain(headers(&incoming))
                .enumerate()
            {
                candidates[index] = Some((header.semantic_identity(), header.signature, header));
            }
            candidates.sort_by_key(|candidate| {
                candidate.map(|(identity, signature, _)| (identity, signature))
            });
            let mut distinct = candidates.into_iter().flatten();
            let (first_identity, _, first) =
                distinct.next().expect("equal sequence join has evidence");
            let (_, _, second) = distinct
                .find(|(identity, _, _)| *identity != first_identity)
                .expect("different semantics join has two witnesses");
            (first, second)
        };
        if matches!(&*existing, LaneState::Equivocated { witnesses: retained } if retained[0] == *first && retained[1] == *second)
        {
            return MergeOutcome::Unchanged;
        }
        let witnesses = [first.clone(), second.clone()];
        *existing = LaneState::Equivocated { witnesses };
        MergeOutcome::EquivocationRetained
    }

    /// Driver has durably allocated the sequence before calling this boundary.
    pub fn merge_local(&mut self, incoming: Representation) -> Result<MergeOutcome, RegisterError> {
        let writer = incoming.original.writer;
        let sequence = incoming.original.writer_seq;
        if let Some(existing) = self.lanes.get(&writer) {
            assert!(
                sequence >= existing.writer_seq(),
                "local producer reused a stale sequence"
            );
            if sequence == existing.writer_seq() {
                assert!(
                    matches!(existing, LaneState::Current { representation } if representation.original.same_semantics(&incoming.original)),
                    "local producer equivocated at an allocated sequence"
                );
            }
        }
        self.merge(incoming)
    }
}

#[cfg(test)]
mod tests;
