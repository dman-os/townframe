//! Explicit native processor-domain metadata (ADR 010 §6).
//!
//! Management supplies the document/group reference and provisions its generic
//! JWK facet separately. Lookup never creates a document or a key. This document
//! names the shared manifest/configuration inputs and contains no slots, task
//! history, duplicated manifest declarations, or node-local settings.

use crate::blobs::encrypt::JwkOct;
use crate::interlude::*;
use crate::tasks::pool::PoolReference;
use crate::tasks::storage::{PinnedJwk, RegisterBinding};
use automerge::{ObjType, ReadDoc, ScalarValue, Value, transaction::Transactable};
use big_repo::{CoordinationError, DocLookup};
use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};
use std::collections::BTreeSet;

pub(crate) mod control;
pub use control::ProcessorRotationControl;
const METADATA: &str = "org.example.daybook.processorConfig/main";
const PROTOCOL: &str = "daybook/triage-processor-domain/v4";

/// Shared execution policy locations; local enablement is not invocation input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorConfiguration {
    pub manifest: url::Url,
    pub config_documents: HashMap<String, String>,
    /// Finite authorization lifetime for each newly originated task, not the
    /// lifetime of the processor obligation or its durable workflow state.
    /// Successors receive new task identities; transport rotation does not renew it.
    pub task_lifetime_secs: std::num::NonZeroU64,
}

impl ProcessorConfiguration {
    /// Resolve the configured lifetime once when originating a new task. Store
    /// this absolute deadline with that task; replay and rotation must reuse it.
    pub fn task_deadline_secs(&self, originated_at_secs: i64) -> i64 {
        let lifetime = i64::try_from(self.task_lifetime_secs.get())
            .expect("validated processor task lifetime fits the timestamp range");
        originated_at_secs
            .checked_add(lifetime)
            .expect("processor task deadline exceeds the timestamp range")
    }
}

/// Stable management-selected authority locus, not a discovery hint or a grant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProcessorDomainReference {
    pub document: DocumentId,
    pub authority_group: [u8; 32],
}

/// An immutable creation/recovery proposal retained by the node plug configuration.
/// Document identity breaks same-generation ties, never CRDT list order or availability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProcessorConfigCandidate {
    pub generation: u64,
    pub reference: ProcessorDomainReference,
}

/// Exact metadata read, authorized at final native Read admission. RegisterStore
/// checks current authority again when opening and using the register.
pub struct ProcessorDomainSnapshot {
    pub reference: ProcessorDomainReference,
    pub heads: Vec<automerge::ChangeHash>,
    pub processor_full_id: String,
    pub configuration: ProcessorConfiguration,
    pub pool: PoolReference,
    pub register_binding: RegisterBinding,
}

/// Management-retained input, not an authority grant. Recovery independently
/// admits current destination Edit and never follows from a lookup miss.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessorDomainRecovery {
    reference: ProcessorDomainReference,
    metadata: Metadata,
}

impl ProcessorDomainSnapshot {
    pub(crate) async fn transport(
        &self,
        repo: &big_repo::BigRepo,
    ) -> Res<Arc<control::ProcessorTransportSnapshot>> {
        Ok(Arc::new(control::ProcessorTransportSnapshot {
            reference: self.reference.clone(),
            heads: self.heads.clone(),
            processor_full_id: self.processor_full_id.clone(),
            register_binding: self.register_binding(repo).await?,
        }))
    }

    pub fn recovery_metadata(&self) -> ProcessorDomainRecovery {
        ProcessorDomainRecovery {
            reference: self.reference.clone(),
            metadata: Metadata {
                protocol: PROTOCOL.into(),
                rotation_generation: self.pool.rotation_generation,
                processor_full_id: self.processor_full_id.clone(),
                configuration: self.configuration.clone(),
                authority_group: self.reference.authority_group,
                pool_document: self.pool.document.to_string(),
                pool_authority_group: self.pool.authority_group,
                slot_key: self.register_binding.publication_key.clone(),
                predecessor_parts: self.register_binding.predecessor_parts.clone(),
                allowed_slot_keys: self.register_binding.allowed_key_refs.clone(),
                historical_key_authorities: self
                    .register_binding
                    .historical_key_authorities
                    .clone(),
            },
        }
    }

    /// Provisioned metadata is a binding hint, not the sender's Read grant.
    /// Opaque storage opens with the receiving node's current Relay authority.
    pub(crate) async fn register_binding(&self, repo: &big_repo::BigRepo) -> Res<RegisterBinding> {
        eyre::ensure!(
            !self.processor_full_id.is_empty(),
            "empty processor identity"
        );
        let identity = register_identity(&self.processor_full_id);
        let binding = &self.register_binding;
        eyre::ensure!(
            binding.scope.as_slice() == identity
                && binding.generation == self.pool.rotation_generation
                && binding.part
                    == processor_part(&self.processor_full_id, self.pool.rotation_generation)
                && binding.incarnation == identity
                && binding.authority.document() == &self.reference.document
                && binding.authority.group() == &self.reference.authority_group,
            "processor metadata is detached from its native register binding"
        );
        RegisterBinding::validate_keys(
            &self.reference.document,
            &binding.scope,
            &binding.allowed_key_refs,
            &binding.publication_key,
            &binding.historical_key_authorities,
        )?;
        let sampled = repo
            .coordination_authority(
                self.reference.document.clone(),
                self.reference.authority_group,
            )
            .await?;
        let authority = repo
            .admit_coordination_access(&sampled, big_repo::keyhive_core::access::Access::Relay)
            .await?;
        Ok(RegisterBinding {
            generation: binding.generation,
            predecessor_parts: binding.predecessor_parts.clone(),
            historical_key_authorities: binding.historical_key_authorities.clone(),
            scope: binding.scope.clone(),
            part: binding.part.clone(),
            authority,
            allowed_key_refs: binding.allowed_key_refs.clone(),
            publication_key: binding.publication_key.clone(),
            incarnation: binding.incarnation,
        })
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Ready owns the native snapshot inline without a per-query heap allocation"
)]
pub enum ProcessorDomainLoad {
    /// Explicit binding whose document, key history, or authority is not local yet.
    Pending {
        document: DocumentId,
        /// Native membership sampling raced; retain the authority idle recheck.
        authority_pending: bool,
    },
    Ready(ProcessorDomainSnapshot),
    Rejected(ProcessorDomainRejection),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProcessorDomainRejection {
    #[error("unsupported processor-domain protocol: {0}")]
    UnsupportedProtocol(String),
    #[error("processor-domain metadata names another processor")]
    WrongProcessor,
    #[error("processor-domain metadata names another authority group")]
    WrongAuthority,
    #[error("conflicting processor-domain metadata or slot JWK")]
    Conflict,
    #[error("current document/group authority does not permit Read")]
    Unauthorized,
    #[error("malformed processor-domain metadata: {0}")]
    Malformed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorDomainProvisionError {
    #[error("processor-domain document or key history is unavailable locally: {0}")]
    Pending(DocumentId),
    #[error(transparent)]
    Metadata(#[from] ProcessorDomainRejection),
    #[error(transparent)]
    Authority(#[from] CoordinationError),
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Metadata {
    protocol: String,
    rotation_generation: u64,
    processor_full_id: String,
    configuration: ProcessorConfiguration,
    authority_group: [u8; 32],
    pool_document: String,
    pool_authority_group: [u8; 32],
    slot_key: PinnedJwk,
    predecessor_parts: BTreeSet<PartKey>,
    allowed_slot_keys: BTreeSet<url::Url>,
    historical_key_authorities: std::collections::BTreeMap<String, [u8; 32]>,
}

impl Metadata {
    fn validate(
        &self,
        reference: &ProcessorDomainReference,
        processor_full_id: &str,
    ) -> Result<PoolReference, ProcessorDomainRejection> {
        if self.protocol != PROTOCOL {
            return Err(ProcessorDomainRejection::UnsupportedProtocol(
                self.protocol.clone(),
            ));
        }
        if self.processor_full_id.is_empty() {
            return Err(malformed("empty processor identity"));
        }
        if self.processor_full_id != processor_full_id {
            return Err(ProcessorDomainRejection::WrongProcessor);
        }
        if self.authority_group != reference.authority_group {
            return Err(ProcessorDomainRejection::WrongAuthority);
        }
        let (plug, _) = processor_full_id
            .rsplit_once('/')
            .ok_or_else(|| malformed("processor identifier has no plug boundary"))?;
        crate::plugs::PlugsRepo::parse_enabled_ref(&self.configuration.manifest)
            .map_err(malformed)?;
        i64::try_from(self.configuration.task_lifetime_secs.get()).map_err(malformed)?;
        if !self.configuration.config_documents.contains_key(plug) {
            return Err(malformed(
                "processor owner has no shared configuration association",
            ));
        }
        for document in self.configuration.config_documents.values() {
            let document: DocumentId = document.parse().map_err(malformed)?;
            document.to_bytes32().map_err(malformed)?;
        }
        reference.document.to_bytes32().map_err(malformed)?;
        let document: DocumentId = self.pool_document.parse().map_err(malformed)?;
        document.to_bytes32().map_err(malformed)?;
        if document != reference.document || self.pool_authority_group != reference.authority_group
        {
            return Err(malformed(
                "processor and pool configuration must share their authority document",
            ));
        }
        RegisterBinding::validate_keys(
            &reference.document,
            &register_identity(processor_full_id),
            &self.allowed_slot_keys,
            &self.slot_key,
            &self.historical_key_authorities,
        )
        .map_err(malformed)?;
        Ok(PoolReference {
            rotation_generation: self.rotation_generation,
            document,
            authority_group: self.pool_authority_group,
        })
    }
}

/// Stateless native repository handle. There is deliberately no rendezvous,
/// implicit management discovery, or lookup-miss provisioning path.
#[derive(Clone)]
pub struct ProcessorDomainRepo {
    big_repo: SharedBigRepo,
    actor: ActorId,
}

impl ProcessorDomainRepo {
    pub fn new(big_repo: SharedBigRepo, actor: ActorId) -> Self {
        Self { big_repo, actor }
    }

    /// Retain concurrent proposals in the plug-config authority, even when their
    /// referenced documents are unavailable. Publishing never activates a processor.
    pub async fn publish_candidate(
        &self,
        plug_config: &DocumentId,
        processor: &str,
        candidate: ProcessorConfigCandidate,
    ) -> Res<ProcessorConfigCandidate> {
        candidate.reference.document.to_bytes32()?;
        let handle = match self.big_repo.get_doc(plug_config).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            _ => return Err(CoordinationError::Pending.into()),
        };
        let json = serde_json::to_string(&candidate)?;
        handle
            .with_document(|doc| -> Res<()> {
                let candidates = Self::candidates(doc, processor)?;
                if candidates.contains(&candidate) {
                    return Ok(());
                }
                doc.set_actor(self.actor.clone());
                let mut tx = doc.transaction();
                let facets = match tx.get(automerge::ROOT, "facets")? {
                    Some((Value::Object(ObjType::Map), facets)) => facets,
                    None => tx.put_object(automerge::ROOT, "facets", ObjType::Map)?,
                    Some(_) => eyre::bail!("plug configuration facets must be a map"),
                };
                let bindings =
                    match tx.get(&facets, "org.example.daybook.processorBindings/main")? {
                        Some((Value::Object(ObjType::Map), bindings)) => bindings,
                        None => tx.put_object(
                            &facets,
                            "org.example.daybook.processorBindings/main",
                            ObjType::Map,
                        )?,
                        Some(_) => eyre::bail!("processor bindings must be a map"),
                    };
                let list = match tx.get(&bindings, processor)? {
                    Some((Value::Object(ObjType::List), list)) => list,
                    None => tx.put_object(&bindings, processor, ObjType::List)?,
                    Some(_) => eyre::bail!("processor candidates must be a list"),
                };
                tx.insert(&list, tx.length(&list), json)?;
                tx.commit();
                Ok(())
            })
            .await??;
        self.selected_candidate(plug_config, processor)
            .await?
            .ok_or_eyre("published processor candidate disappeared")
    }

    pub(crate) async fn configuration_candidates(
        &self,
        plug_config: &DocumentId,
        processor: &str,
    ) -> Res<BTreeSet<ProcessorConfigCandidate>> {
        let handle = match self.big_repo.get_doc(plug_config).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            _ => return Err(CoordinationError::Pending.into()),
        };
        handle
            .with_document_read(|doc| Self::candidates(doc, processor))
            .await
    }

    pub async fn selected_candidate(
        &self,
        plug_config: &DocumentId,
        processor: &str,
    ) -> Res<Option<ProcessorConfigCandidate>> {
        let handle = match self.big_repo.get_doc(plug_config).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            _ => return Err(CoordinationError::Pending.into()),
        };
        handle
            .with_document_read(|doc| -> Res<_> {
                Ok(Self::candidates(doc, processor)?
                    .into_iter()
                    .max_by(|left, right| {
                        left.generation
                            .cmp(&right.generation)
                            .then_with(|| right.reference.cmp(&left.reference))
                    }))
            })
            .await
    }

    fn candidates(doc: &impl ReadDoc, processor: &str) -> Res<BTreeSet<ProcessorConfigCandidate>> {
        let mut candidates = BTreeSet::new();
        // Traverse all concurrent containers: winner hydration would erase proposals.
        for (value, facets) in doc.get_all(automerge::ROOT, "facets")? {
            eyre::ensure!(
                matches!(value, Value::Object(ObjType::Map)),
                "facets must be a map"
            );
            for (value, bindings) in
                doc.get_all(&facets, "org.example.daybook.processorBindings/main")?
            {
                eyre::ensure!(
                    matches!(value, Value::Object(ObjType::Map)),
                    "processor bindings must be a map"
                );
                for (value, list) in doc.get_all(&bindings, processor)? {
                    eyre::ensure!(
                        matches!(value, Value::Object(ObjType::List)),
                        "processor candidates must be a list"
                    );
                    for index in 0..doc.length(&list) {
                        for (value, _) in doc.get_all(&list, index)? {
                            let Value::Scalar(value) = value else {
                                eyre::bail!("candidate must be JSON");
                            };
                            let ScalarValue::Str(json) = value.as_ref() else {
                                eyre::bail!("candidate must be JSON");
                            };
                            let candidate: ProcessorConfigCandidate = serde_json::from_str(json)?;
                            candidate.reference.document.to_bytes32()?;
                            candidates.insert(candidate);
                        }
                    }
                }
            }
        }
        Ok(candidates)
    }

    pub async fn load(
        &self,
        reference: &ProcessorDomainReference,
        processor_full_id: &str,
    ) -> Res<ProcessorDomainLoad> {
        let handle = match self.big_repo.get_doc(&reference.document).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                return Ok(pending(&reference.document));
            }
        };
        // Do not disclose private metadata/errors before named-group Read. All
        // payload/graph reads precede the final accepted-operation admission.
        let (prepared, heads) = handle
            .with_document_read(|doc| {
                let prepared = (|| {
                    let metadata = read_metadata(doc)?.ok_or_else(|| {
                        malformed("referenced processor-domain metadata is absent")
                    })?;
                    let pool = metadata.validate(reference, processor_full_id)?;
                    let key_ready = validate_pinned_key(doc, &metadata.slot_key)?;
                    Ok::<_, ProcessorDomainRejection>((metadata, pool, key_ready))
                })();
                (prepared, doc.get_heads())
            })
            .await;
        let sampled = match self
            .big_repo
            .coordination_authority(reference.document.clone(), reference.authority_group)
            .await
        {
            Ok(authority) => authority,
            Err(error) => return authority_status(error, &reference.document),
        };
        let authority = match self.big_repo.admit_coordination_read(&sampled).await {
            Ok(authority) => authority,
            Err(error) => return authority_status(error, &reference.document),
        };
        let (metadata, pool, key_ready) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Ok(ProcessorDomainLoad::Rejected(error)),
        };
        if !key_ready {
            return Ok(pending(&reference.document));
        }
        let identity = register_identity(&metadata.processor_full_id);
        Ok(ProcessorDomainLoad::Ready(ProcessorDomainSnapshot {
            reference: reference.clone(),
            heads,
            pool,
            register_binding: RegisterBinding {
                generation: metadata.rotation_generation,
                predecessor_parts: metadata.predecessor_parts,
                historical_key_authorities: metadata.historical_key_authorities,
                scope: identity.to_vec(),
                part: processor_part(&metadata.processor_full_id, metadata.rotation_generation),
                authority,
                allowed_key_refs: metadata.allowed_slot_keys,
                publication_key: metadata.slot_key,
                incarnation: identity,
            },
            processor_full_id: metadata.processor_full_id,
            configuration: metadata.configuration,
        }))
    }

    /// Resolve the shared policy from the exact captured configuration revision.
    /// Historical heads witness input; current native Read still gates release.
    pub(crate) async fn configuration_at(
        &self,
        reference: &ProcessorDomainReference,
        processor: &str,
        heads: &ChangeHashSet,
    ) -> Res<Option<ProcessorConfiguration>> {
        let DocLookup::Ready(handle) = self.big_repo.get_doc(&reference.document).await? else {
            return Ok(None);
        };
        if handle.is_partially_decrypted() {
            return Ok(None);
        }
        let configuration = handle
            .with_document_read(|doc| -> Res<_> {
                if heads
                    .0
                    .iter()
                    .any(|head| doc.get_change_by_hash(head).is_none())
                {
                    return Ok(None);
                }
                let Some(metadata) = read_metadata_at(doc, Some(&heads.0))? else {
                    return Ok(None);
                };
                metadata.validate(reference, processor)?;
                Ok(Some(metadata.configuration))
            })
            .await?;
        let authority = self
            .big_repo
            .coordination_authority(reference.document.clone(), reference.authority_group)
            .await?;
        self.big_repo
            .admit_coordination_access(&authority, big_repo::keyhive_core::access::Access::Read)
            .await?;
        Ok(configuration)
    }

    /// Bind a generic JWK already provisioned in this existing native document.
    /// Exact key heads are retained, never replaced with latest document heads.
    /// Repeating identical metadata is idempotent. Management may change the pool
    /// or key binding, but cannot repurpose a domain for another processor/group.
    pub async fn provision(
        &self,
        reference: &ProcessorDomainReference,
        processor_full_id: String,
        configuration: ProcessorConfiguration,
        pool: PoolReference,
        slot_key: PinnedJwk,
    ) -> Result<(), ProcessorDomainProvisionError> {
        let metadata = Metadata {
            protocol: PROTOCOL.into(),
            rotation_generation: pool.rotation_generation,
            processor_full_id,
            configuration,
            authority_group: reference.authority_group,
            pool_document: pool.document.to_string(),
            pool_authority_group: pool.authority_group,
            allowed_slot_keys: BTreeSet::from([slot_key.key_ref.clone()]),
            predecessor_parts: BTreeSet::new(),
            historical_key_authorities: std::collections::BTreeMap::new(),
            slot_key,
        };
        self.provision_metadata(reference, metadata).await
    }

    /// Preserve predecessor slot evidence when selecting a fresh native configuration.
    pub async fn provision_successor(
        &self,
        predecessor: &ProcessorDomainRecovery,
        reference: &ProcessorDomainReference,
        pool: PoolReference,
        slot_key: PinnedJwk,
    ) -> Result<(), ProcessorDomainProvisionError> {
        predecessor.metadata.validate(
            &predecessor.reference,
            &predecessor.metadata.processor_full_id,
        )?;
        let expected = predecessor
            .metadata
            .rotation_generation
            .checked_add(1)
            .expect("processor configuration generation exhausted");
        if pool.rotation_generation != expected {
            return Err(malformed("successor skips proposed generation").into());
        }
        if reference.authority_group != predecessor.reference.authority_group {
            return Err(ProcessorDomainRejection::WrongAuthority.into());
        }
        let mut metadata = Metadata {
            protocol: PROTOCOL.into(),
            rotation_generation: expected,
            processor_full_id: predecessor.metadata.processor_full_id.clone(),
            configuration: predecessor.metadata.configuration.clone(),
            authority_group: reference.authority_group,
            pool_document: pool.document.to_string(),
            pool_authority_group: pool.authority_group,
            slot_key: slot_key.clone(),
            predecessor_parts: predecessor.metadata.predecessor_parts.clone(),
            allowed_slot_keys: predecessor.metadata.allowed_slot_keys.clone(),
            historical_key_authorities: predecessor.metadata.historical_key_authorities.clone(),
        };
        metadata.predecessor_parts.insert(processor_part(
            &metadata.processor_full_id,
            predecessor.metadata.rotation_generation,
        ));
        metadata.historical_key_authorities.insert(
            predecessor.reference.document.to_string(),
            predecessor.reference.authority_group,
        );
        metadata.allowed_slot_keys.insert(slot_key.key_ref.clone());
        metadata.slot_key = slot_key;
        self.provision_metadata(reference, metadata).await
    }

    async fn provision_metadata(
        &self,
        reference: &ProcessorDomainReference,
        metadata: Metadata,
    ) -> Result<(), ProcessorDomainProvisionError> {
        metadata.validate(reference, &metadata.processor_full_id)?;
        let json = serde_json::to_string(&metadata).map_err(eyre::Report::from)?;
        let handle = match self.big_repo.get_doc(&reference.document).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                return Err(ProcessorDomainProvisionError::Pending(
                    reference.document.clone(),
                ));
            }
        };
        let existing = handle
            .with_document_read(|doc| {
                let existing = read_metadata(doc)?;
                if let Some(existing) = &existing {
                    existing.validate(reference, &metadata.processor_full_id)?;
                }
                Ok::<_, ProcessorDomainRejection>((
                    existing,
                    validate_pinned_key(doc, &metadata.slot_key)?,
                ))
            })
            .await?;
        if !existing.1 {
            return Err(ProcessorDomainProvisionError::Pending(
                reference.document.clone(),
            ));
        }
        let sampled = self
            .big_repo
            .coordination_authority(reference.document.clone(), reference.authority_group)
            .await?;
        // Native exact document/group Edit admission linearizes this operation;
        // the ordinary document commit separately enforces Edit and durability.
        self.big_repo.admit_coordination(&sampled).await?;
        handle
            .with_document(|doc| {
                if read_metadata(doc)? != existing.0 {
                    return Err(ProcessorDomainRejection::Conflict.into());
                }
                if !validate_pinned_key(doc, &metadata.slot_key)? {
                    return Err(ProcessorDomainProvisionError::Pending(
                        reference.document.clone(),
                    ));
                }
                if existing.0.as_ref() == Some(&metadata) {
                    return Ok(());
                }
                doc.set_actor(self.actor.clone());
                let mut tx = doc.transaction();
                let facets = match tx
                    .get(automerge::ROOT, "facets")
                    .map_err(eyre::Report::from)?
                {
                    Some((Value::Object(ObjType::Map), facets)) => facets,
                    None => tx
                        .put_object(automerge::ROOT, "facets", ObjType::Map)
                        .map_err(eyre::Report::from)?,
                    Some(_) => return Err(malformed("facets must be a map").into()),
                };
                let config = match tx.get(&facets, METADATA).map_err(eyre::Report::from)? {
                    Some((Value::Object(ObjType::Map), config)) => config,
                    None => tx
                        .put_object(&facets, METADATA, ObjType::Map)
                        .map_err(eyre::Report::from)?,
                    Some(_) => return Err(malformed("processor config facet must be a map").into()),
                };
                tx.put(config, "binding", json.as_str())
                    .map_err(eyre::Report::from)?;
                tx.commit();
                Ok::<_, ProcessorDomainProvisionError>(())
            })
            .await??;
        Ok(())
    }
}

fn malformed(error: impl std::fmt::Display) -> ProcessorDomainRejection {
    ProcessorDomainRejection::Malformed(error.to_string())
}

fn pending(document: &DocumentId) -> ProcessorDomainLoad {
    ProcessorDomainLoad::Pending {
        document: document.clone(),
        authority_pending: false,
    }
}

fn authority_status(error: CoordinationError, document: &DocumentId) -> Res<ProcessorDomainLoad> {
    match error {
        CoordinationError::Pending => Ok(ProcessorDomainLoad::Pending {
            document: document.clone(),
            authority_pending: true,
        }),
        CoordinationError::Unauthorized => Ok(ProcessorDomainLoad::Rejected(
            ProcessorDomainRejection::Unauthorized,
        )),
        CoordinationError::Invalid(reason) => Ok(ProcessorDomainLoad::Rejected(malformed(reason))),
        CoordinationError::Other(error) => Err(error),
    }
}

/// Logical register, default transport part, and representation incarnation all
/// depend only on stable processor identity, never on document or key version.
fn register_identity(processor_full_id: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"daybook/triage-processor-slots/v1");
    hash.update(&(processor_full_id.len() as u64).to_be_bytes());
    hash.update(processor_full_id.as_bytes());
    *hash.finalize().as_bytes()
}

fn processor_part(processor: &str, generation: u64) -> PartKey {
    let identity = register_identity(processor);
    if generation == 0 {
        return PartKey::new(identity);
    }
    let prefix = b"daybook/processor-part/v1\0";
    let mut bytes = Vec::with_capacity(prefix.len() + identity.len() + 8);
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&identity);
    bytes.extend_from_slice(&generation.to_be_bytes());
    PartKey::new(bytes)
}

fn read_metadata(doc: &impl ReadDoc) -> Result<Option<Metadata>, ProcessorDomainRejection> {
    read_metadata_at(doc, None)
}

fn read_metadata_at(
    doc: &impl ReadDoc,
    heads: Option<&[automerge::ChangeHash]>,
) -> Result<Option<Metadata>, ProcessorDomainRejection> {
    let read = |object: &automerge::ObjId, property: &str| match heads {
        Some(heads) => doc.get_all_at(object, property, heads),
        None => doc.get_all(object, property),
    };
    let mut metadata = None;
    let facets = read(&automerge::ROOT, "facets").map_err(malformed)?;
    if facets.len() > 1 {
        return Err(ProcessorDomainRejection::Conflict);
    }
    let Some((Value::Object(ObjType::Map), facets)) = facets.first() else {
        return if facets.is_empty() {
            Ok(None)
        } else {
            Err(malformed("facets must be a map"))
        };
    };
    let configs = read(facets, METADATA).map_err(malformed)?;
    if configs.len() > 1 {
        return Err(ProcessorDomainRejection::Conflict);
    }
    let Some((Value::Object(ObjType::Map), config)) = configs.first() else {
        return if configs.is_empty() {
            Ok(None)
        } else {
            Err(malformed("processor config facet must be a map"))
        };
    };
    for (value, _) in read(config, "binding").map_err(malformed)? {
        let Value::Scalar(value) = value else {
            return Err(malformed("processor-domain metadata must be complete JSON"));
        };
        let ScalarValue::Str(json) = value.as_ref() else {
            return Err(malformed("processor-domain metadata must be complete JSON"));
        };
        let candidate = serde_json::from_str::<Metadata>(json).map_err(malformed)?;
        if metadata
            .as_ref()
            .is_some_and(|existing| existing != &candidate)
        {
            return Err(ProcessorDomainRejection::Conflict);
        }
        metadata = Some(candidate);
    }
    Ok(metadata)
}

/// False means historical heads/facet are not materialized. Decode through the
/// same generic JWK codec as RegisterStore; never read latest key material.
fn validate_pinned_key(
    doc: &automerge::Automerge,
    pinned: &PinnedJwk,
) -> Result<bool, ProcessorDomainRejection> {
    if pinned
        .heads
        .0
        .iter()
        .any(|head| doc.get_change_by_hash(head).is_none())
    {
        return Ok(false);
    }
    let reference = daybook_types::url::parse_facet_ref(&pinned.key_ref).map_err(malformed)?;
    let facet_key = reference.facet_key.to_string();
    // Winner hydration must not hide competing facet objects or JWK members.
    let facets = doc
        .get_all_at(automerge::ROOT, "facets", &pinned.heads.0)
        .map_err(malformed)?;
    if facets.len() > 1 {
        return Err(ProcessorDomainRejection::Conflict);
    }
    let Some((Value::Object(ObjType::Map), facets)) = facets.first() else {
        return if facets.is_empty() {
            Ok(false)
        } else {
            Err(malformed("facets must be a map"))
        };
    };
    let values = doc
        .get_all_at(facets, facet_key.as_str(), &pinned.heads.0)
        .map_err(malformed)?;
    if values.len() > 1 {
        return Err(ProcessorDomainRejection::Conflict);
    }
    let Some((Value::Object(ObjType::Map), key)) = values.first() else {
        return if values.is_empty() {
            Ok(false)
        } else {
            Err(malformed("JWK facet must be a map"))
        };
    };
    for field in ["kty", "k"] {
        if doc
            .get_all_at(key, field, &pinned.heads.0)
            .map_err(malformed)?
            .len()
            > 1
        {
            return Err(ProcessorDomainRejection::Conflict);
        }
    }
    let raw: Option<am_utils_rs::codecs::ThroughJson<serde_json::Value>> =
        autosurgeon::hydrate_path_at(
            doc,
            &automerge::ROOT,
            vec![
                autosurgeon::Prop::Key("facets".into()),
                autosurgeon::Prop::Key(facet_key.into()),
            ],
            &pinned.heads.0,
        )
        .map_err(malformed)?;
    let Some(raw) = raw else {
        return Ok(false);
    };
    let WellKnownFacet::Jwk(jwk) =
        WellKnownFacet::from_json(raw.0, WellKnownFacetTag::Jwk).map_err(malformed)?
    else {
        unreachable!("JWK decoder returns JWK")
    };
    serde_json::from_value::<JwkOct>(serde_json::to_value(jwk).map_err(malformed)?)
        .map_err(malformed)?
        .to_master_key()
        .map_err(malformed)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blobs::encrypt::MasterKey;
    use crate::tasks::storage::RegisterStore;
    use big_sync::SqlitePartStore;
    use big_sync_core::encrypted_register::LaneState;
    use daybook_types::doc::FacetKey;

    #[test]
    fn processor_task_lifetime_is_explicit_and_origin_bound() -> Res<()> {
        let configuration = ProcessorConfiguration {
            manifest: daybook_types::url::build_facet_ref(
                &DocumentId::new([1; 32]).to_string(),
                &FacetKey::from(WellKnownFacetTag::PlugManifest),
            )?,
            config_documents: [("plug".into(), DocumentId::new([1; 32]).to_string())].into(),
            task_lifetime_secs: std::num::NonZeroU64::new(60).unwrap(),
        };
        assert_eq!(configuration.task_deadline_secs(100), 160);
        assert_eq!(
            serde_json::from_slice::<ProcessorConfiguration>(&serde_json::to_vec(&configuration)?)?,
            configuration
        );
        let mut encoded = serde_json::to_value(&configuration)?;
        encoded["task_lifetime_secs"] = serde_json::json!(0);
        assert!(serde_json::from_value::<ProcessorConfiguration>(encoded.clone()).is_err());
        encoded
            .as_object_mut()
            .unwrap()
            .remove("task_lifetime_secs")
            .unwrap();
        assert!(serde_json::from_value::<ProcessorConfiguration>(encoded).is_err());
        Ok(())
    }

    fn write_key(doc: &mut automerge::Automerge, key: &MasterKey) -> Res<()> {
        let mut tx = doc.transaction();
        let facets = match tx.get(automerge::ROOT, "facets")? {
            Some((Value::Object(ObjType::Map), facets)) => facets,
            None => tx.put_object(automerge::ROOT, "facets", ObjType::Map)?,
            Some(_) => eyre::bail!("fixture facets must be a map"),
        };
        autosurgeon::reconcile_prop(
            &mut tx,
            &facets,
            autosurgeon::Prop::Key(FacetKey::from(WellKnownFacetTag::Jwk).to_string().into()),
            am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(key))?),
        )?;
        tx.commit();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_candidate_containers_select_missing_winner_independent_of_arrival()
    -> Res<()> {
        let (repo, _sync, stop) = crate::test_support::boot_repo().await?;
        let mut base = automerge::Automerge::new();
        let mut tx = base.transaction();
        tx.put(automerge::ROOT, "version", 1)?;
        tx.commit();
        let candidates: Vec<_> = [13, 12]
            .into_iter()
            .map(|byte| ProcessorConfigCandidate {
                generation: 2,
                reference: ProcessorDomainReference {
                    document: DocumentId::new([byte; 32]),
                    authority_group: [4; 32],
                },
            })
            .collect();
        let mut branches = Vec::new();
        for (index, candidate) in candidates.iter().enumerate() {
            let mut branch = base.fork();
            branch.set_actor(ActorId::from(vec![index as u8]));
            let mut tx = branch.transaction();
            let facets = tx.put_object(automerge::ROOT, "facets", ObjType::Map)?;
            let bindings = tx.put_object(
                facets,
                "org.example.daybook.processorBindings/main",
                ObjType::Map,
            )?;
            let list = tx.put_object(bindings, "plug/processor", ObjType::List)?;
            tx.insert(list, 0, serde_json::to_string(candidate)?)?;
            tx.commit();
            branches.push(branch);
        }
        let domains =
            ProcessorDomainRepo::new(Arc::clone(&repo), ActorId::from(b"selector".as_slice()));
        for reverse in [false, true] {
            let mut merged = base.clone();
            let order = if reverse { [1, 0] } else { [0, 1] };
            for index in order {
                merged.merge(&mut branches[index].clone())?;
            }
            assert_eq!(
                ProcessorDomainRepo::candidates(&merged, "plug/processor")?,
                candidates.iter().cloned().collect()
            );
            let config = repo.create_doc(merged).await?;
            assert_eq!(
                domains
                    .selected_candidate(&config.document_id(), "plug/processor")
                    .await?,
                Some(candidates[1].clone())
            );
            let successor = ProcessorConfigCandidate {
                generation: 3,
                reference: ProcessorDomainReference {
                    document: DocumentId::new([99; 32]),
                    authority_group: [4; 32],
                },
            };
            assert_eq!(
                domains
                    .publish_candidate(&config.document_id(), "plug/processor", successor.clone())
                    .await?,
                successor
            );
            assert!(matches!(
                domains.load(&successor.reference, "plug/processor").await?,
                ProcessorDomainLoad::Pending { .. }
            ));
        }
        stop().await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retained_management_metadata_recovers_without_original_document() -> Res<()> {
        let (repo, _sync, stop) = crate::test_support::boot_repo().await?;
        let group = repo.create_group_with_parents(vec![]).await?;
        repo.add_admin_member_to_group(repo.local_keyhive_agent().await?, &group)
            .await?;
        let mut doc = automerge::Automerge::new();
        write_key(&mut doc, &MasterKey::random())?;
        let handle = repo
            .create_doc_with_parents(doc, vec![group.clone().into()])
            .await?;
        let destination = ProcessorDomainReference {
            document: handle.document_id(),
            authority_group: group.id().to_bytes(),
        };
        let missing = ProcessorDomainReference {
            document: DocumentId::new([4; 32]),
            authority_group: destination.authority_group,
        };
        let facet = FacetKey::from(WellKnownFacetTag::Jwk);
        let slot_key = PinnedJwk {
            key_ref: daybook_types::url::build_facet_ref(
                &destination.document.to_string(),
                &facet,
            )?,
            heads: handle
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await,
        };
        let old_key = PinnedJwk {
            key_ref: daybook_types::url::build_facet_ref(&missing.document.to_string(), &facet)?,
            heads: slot_key.heads.clone(),
        };
        let retained = ProcessorDomainRecovery {
            reference: missing.clone(),
            metadata: Metadata {
                protocol: PROTOCOL.into(),
                rotation_generation: 0,
                processor_full_id: "plug/processor".into(),
                configuration: ProcessorConfiguration {
                    manifest: daybook_types::url::build_facet_ref(
                        &missing.document.to_string(),
                        &FacetKey::from(WellKnownFacetTag::PlugManifest),
                    )?,
                    config_documents: [("plug".into(), missing.document.to_string())].into(),
                    task_lifetime_secs: std::num::NonZeroU64::new(3600).unwrap(),
                },
                authority_group: missing.authority_group,
                pool_document: missing.document.to_string(),
                pool_authority_group: missing.authority_group,
                slot_key: old_key.clone(),
                predecessor_parts: BTreeSet::new(),
                allowed_slot_keys: [old_key.key_ref].into(),
                historical_key_authorities: Default::default(),
            },
        };
        let retained: ProcessorDomainRecovery =
            serde_json::from_slice(&serde_json::to_vec(&retained)?)?;
        let domains = ProcessorDomainRepo::new(
            Arc::clone(&repo),
            ActorId::from(b"recovery-fixture".as_slice()),
        );
        assert!(matches!(
            domains.load(&missing, "plug/processor").await?,
            ProcessorDomainLoad::Pending { .. }
        ));
        domains
            .provision_successor(
                &retained,
                &destination,
                PoolReference {
                    rotation_generation: 1,
                    document: destination.document.clone(),
                    authority_group: destination.authority_group,
                },
                slot_key,
            )
            .await?;
        let ProcessorDomainLoad::Ready(recovered) =
            domains.load(&destination, "plug/processor").await?
        else {
            eyre::bail!("explicit recovery did not materialize destination metadata");
        };
        assert_eq!(recovered.configuration, retained.metadata.configuration);
        assert_eq!(recovered.pool.rotation_generation, 1);
        assert!(
            recovered
                .register_binding
                .predecessor_parts
                .contains(&processor_part("plug/processor", 0))
        );
        assert_eq!(
            recovered
                .register_binding
                .historical_key_authorities
                .get(&missing.document.to_string()),
            Some(&missing.authority_group)
        );
        assert!(matches!(
            repo.get_doc(&missing.document).await?,
            DocLookup::Missing
        ));
        stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_domain_binding_preserves_identity_and_historical_slot_keys() -> Res<()> {
        let dir = tempfile::tempdir()?;
        let (repo, _sync, stop) =
            crate::test_support::boot_disk_repo(dir.path().join("repo")).await?;
        let group = repo.create_group_with_parents(vec![]).await?;
        repo.add_admin_member_to_group(repo.local_keyhive_agent().await?, &group)
            .await?;
        let mut seed = automerge::Automerge::new();
        write_key(&mut seed, &MasterKey::random())?;
        let handle = repo
            .create_doc_with_parents(seed, vec![group.clone().into()])
            .await?;
        let reference = ProcessorDomainReference {
            document: handle.document_id(),
            authority_group: group.id().to_bytes(),
        };
        let pool = PoolReference {
            rotation_generation: 0,
            document: reference.document.clone(),
            authority_group: group.id().to_bytes(),
        };
        let key = PinnedJwk {
            key_ref: daybook_types::url::build_facet_ref(
                &reference.document.to_string(),
                &FacetKey::from(WellKnownFacetTag::Jwk),
            )?,
            heads: handle
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await,
        };
        let domains = ProcessorDomainRepo::new(
            Arc::clone(&repo),
            ActorId::from(b"domain-fixture".as_slice()),
        );
        let configuration = ProcessorConfiguration {
            manifest: daybook_types::url::build_facet_ref(
                &reference.document.to_string(),
                &FacetKey::from(WellKnownFacetTag::PlugManifest),
            )?,
            config_documents: [("plug".into(), reference.document.to_string())].into(),
            task_lifetime_secs: std::num::NonZeroU64::new(3600).unwrap(),
        };
        let missing = ProcessorDomainReference {
            document: DocumentId::new([4; 32]),
            authority_group: reference.authority_group,
        };
        assert!(
            matches!(domains.load(&missing, "plug/processor").await?, ProcessorDomainLoad::Pending { document, authority_pending: false } if document == missing.document)
        );
        assert!(
            matches!(domains.provision(&missing, "plug/processor".into(), configuration.clone(), PoolReference { rotation_generation: 0, document: missing.document.clone(), authority_group: missing.authority_group }, PinnedJwk {
            key_ref: daybook_types::url::build_facet_ref(&missing.document.to_string(), &FacetKey::from(WellKnownFacetTag::Jwk))?,
            heads: key.heads.clone(),
        }).await, Err(ProcessorDomainProvisionError::Pending(document)) if document == missing.document)
        );
        repo.wait_for_quiescence(None).await?;
        domains
            .provision(
                &reference,
                "plug/processor".into(),
                configuration.clone(),
                pool.clone(),
                key.clone(),
            )
            .await?;
        let ProcessorDomainLoad::Ready(first) = domains.load(&reference, "plug/processor").await?
        else {
            eyre::bail!("native processor domain must load");
        };
        assert_eq!(first.pool, pool);
        assert_eq!(first.register_binding.publication_key, key);
        assert_eq!(
            first.heads,
            handle
                .with_document_read(automerge::Automerge::get_heads)
                .await
        );
        domains
            .provision(
                &reference,
                "plug/processor".into(),
                configuration.clone(),
                pool.clone(),
                key.clone(),
            )
            .await?;
        assert_eq!(
            first.heads,
            handle
                .with_document_read(automerge::Automerge::get_heads)
                .await
        );
        // A later shared association must not rewrite an already captured
        // invocation, even when its replacement document has not arrived.
        let mut changed_configuration = configuration.clone();
        changed_configuration
            .config_documents
            .insert("plug".into(), missing.document.to_string());
        domains
            .provision(
                &reference,
                "plug/processor".into(),
                changed_configuration.clone(),
                pool.clone(),
                key.clone(),
            )
            .await?;
        let old_heads = ChangeHashSet(first.heads.clone().into());
        assert_eq!(
            domains
                .configuration_at(&reference, "plug/processor", &old_heads)
                .await?,
            Some(configuration.clone())
        );
        let changed_heads = handle
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await;
        assert_eq!(
            domains
                .configuration_at(&reference, "plug/processor", &changed_heads)
                .await?,
            Some(changed_configuration)
        );
        domains
            .provision(
                &reference,
                "plug/processor".into(),
                configuration.clone(),
                pool.clone(),
                key.clone(),
            )
            .await?;
        let unrelated_group = repo.create_group_with_parents(vec![]).await?;
        repo.add_admin_member_to_group(repo.local_keyhive_agent().await?, &unrelated_group)
            .await?;
        repo.wait_for_quiescence(None).await?;
        let wrong_authority = ProcessorDomainReference {
            document: reference.document.clone(),
            authority_group: unrelated_group.id().to_bytes(),
        };
        assert!(matches!(
            domains.load(&wrong_authority, "plug/processor").await?,
            ProcessorDomainLoad::Rejected(ProcessorDomainRejection::Unauthorized)
        ));
        assert!(matches!(
            domains.load(&reference, "plug/other").await?,
            ProcessorDomainLoad::Rejected(ProcessorDomainRejection::WrongProcessor)
        ));
        assert!(matches!(
            domains
                .provision(
                    &reference,
                    "plug/other".into(),
                    configuration.clone(),
                    pool.clone(),
                    key.clone()
                )
                .await,
            Err(ProcessorDomainProvisionError::Metadata(
                ProcessorDomainRejection::WrongProcessor
            ))
        ));
        let parts =
            Arc::new(SqlitePartStore::new(repo.sql_ctx(), "processor-domain-fixture", 4).await?);
        let original_identity = (
            first.register_binding.scope.clone(),
            first.register_binding.part.clone(),
            first.register_binding.incarnation,
        );
        let store = RegisterStore::open(
            Arc::clone(&parts),
            Arc::clone(&repo),
            first.register_binding,
        )
        .await?;
        store
            .publish_local(b"slot", vec![], b"before key rotation".to_vec())
            .await?;
        let old = store
            .current(b"slot")
            .await?
            .ok_or_else(|| ferr!("missing published slot"))?;
        let LaneState::Current { representation } =
            old.lanes.values().next().expect("published slot lane")
        else {
            eyre::bail!("published slot must be current");
        };
        handle
            .with_document(|doc| write_key(doc, &MasterKey::random()))
            .await??;
        let rotated_key = PinnedJwk {
            key_ref: key.key_ref.clone(),
            heads: handle
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await,
        };
        let ProcessorDomainLoad::Ready(historical) =
            domains.load(&reference, "plug/processor").await?
        else {
            eyre::bail!("historical domain binding must load");
        };
        assert_eq!(historical.register_binding.publication_key, key);
        assert_eq!(
            store.open_original(representation).await?.body,
            b"before key rotation"
        );
        let changed_pool = pool.clone();
        domains
            .provision(
                &reference,
                "plug/processor".into(),
                configuration.clone(),
                changed_pool.clone(),
                rotated_key.clone(),
            )
            .await?;
        let ProcessorDomainLoad::Ready(rotated) =
            domains.load(&reference, "plug/processor").await?
        else {
            eyre::bail!("rotated domain binding must load");
        };
        assert_eq!(rotated.pool, changed_pool);
        assert_eq!(rotated.register_binding.publication_key, rotated_key);
        assert_eq!(
            (
                rotated.register_binding.scope.clone(),
                rotated.register_binding.part.clone(),
                rotated.register_binding.incarnation
            ),
            original_identity
        );
        store
            .refresh_publication_key(&rotated.register_binding)
            .await?;
        store
            .publish_local(
                b"live-rotation",
                vec![],
                b"new key without reopening".to_vec(),
            )
            .await?;
        let live = store
            .current(b"live-rotation")
            .await?
            .expect("live rotated publication");
        let LaneState::Current {
            representation: live,
        } = live.lanes.values().next().unwrap()
        else {
            unreachable!()
        };
        let mut expected_heads: Vec<_> = rotated_key.heads.iter().map(|head| head.0).collect();
        expected_heads.sort_unstable();
        assert_eq!(live.binding.key_heads, expected_heads);
        assert_eq!(
            store.open_original(live).await?.body,
            b"new key without reopening"
        );
        let reopened = RegisterStore::open(
            Arc::clone(&parts),
            Arc::clone(&repo),
            rotated.register_binding,
        )
        .await?;
        assert_eq!(
            reopened.open_original(representation).await?.body,
            b"before key rotation"
        );
        reopened
            .publish_local(b"slot-two", vec![], b"after key rotation".to_vec())
            .await?;
        handle
            .with_document(|doc| -> Res<()> {
                let mut left = doc.fork();
                let mut right = doc.fork();
                for (branch, document) in [
                    (&mut left, DocumentId::new([6; 32])),
                    (&mut right, DocumentId::new([7; 32])),
                ] {
                    let mut metadata = read_metadata(branch)?.expect("provisioned metadata");
                    metadata.pool_document = document.to_string();
                    let mut tx = branch.transaction();
                    let (_, facets) = tx.get(automerge::ROOT, "facets")?.expect("native facets");
                    let (_, config) = tx.get(&facets, METADATA)?.expect("processor config facet");
                    tx.put(config, "binding", serde_json::to_string(&metadata)?)?;
                    tx.commit();
                }
                doc.merge(&mut left)?;
                doc.merge(&mut right)?;
                Ok(())
            })
            .await??;
        assert!(matches!(
            domains.load(&reference, "plug/processor").await?,
            ProcessorDomainLoad::Rejected(ProcessorDomainRejection::Conflict)
        ));
        assert!(matches!(
            domains
                .provision(
                    &reference,
                    "plug/processor".into(),
                    configuration,
                    pool,
                    key
                )
                .await,
            Err(ProcessorDomainProvisionError::Metadata(
                ProcessorDomainRejection::Conflict
            ))
        ));
        drop(reopened);
        drop(store);
        stop().await?;
        Ok(())
    }
}
