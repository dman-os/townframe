//! Durable pool rendezvous metadata, not an authority registry or a routing driver.
//!
//! A caller provisions the common directory document. References are additive
//! lookup hints; only the referenced document's real Keyhive group binding and
//! current document/group permission intersection authorize descriptor use.

use super::TaskPoolId;
use super::storage::{PinnedJwk, RegisterBinding};
use crate::interlude::*;
use automerge::{ObjType, ReadDoc, ScalarValue, Value, transaction::Transactable};
use big_repo::{BigDocHandle, BigEphemeralTopic, CoordinationError, DocLookup};
use std::collections::BTreeSet;
use tokio::sync::mpsc;
use url::Url;
use utils_rs::prelude::eyre::ensure;

const DIRECTORY: &str = "task_pool_directory";
const DESCRIPTORS: &str = "task_pool_descriptors";
pub const TASK_POOL_DESCRIPTOR_PROTOCOL: &str = "daybook/task-pool/v2";

impl TaskPoolId {
    /// One pool per stable processor, independent of plug upgrades and task parts.
    pub fn for_processor(plug_id: &str, processor_name: &str) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"daybook/processor-pool/v1");
        for value in [plug_id, processor_name] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        Self::from_label(utils_rs::hash::encode_base58_multibase(
            hash.finalize().as_bytes(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RpcTransport {
    IrpcIroh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionClass {
    UntilAuthoritativeRemoval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingDefaults {
    SharedElection,
}

/// Complete low-churn metadata. The enclosing document is the actual CGKA locus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPoolDescriptor {
    pub pool_id: TaskPoolId,
    pub authority_group: [u8; 32],
    pub active_task_part: PartKey,
    /// Logical register identity, independent of the active transport part.
    pub register_scope: Vec<u8>,
    /// Stable representation binding, never evidence of authenticated retirement.
    pub register_incarnation: [u8; 32],
    /// Canonical unpinned JWK facet URLs in the enclosing native authority document.
    pub allowed_key_refs: BTreeSet<Url>,
    /// Provisioned key heads may precede the descriptor commit; never use latest heads.
    pub publication_key: PinnedJwk,
    pub archive_part: Option<PartKey>,
    pub router_slot: ObjKey,
    pub router_heartbeat_topic: BigEphemeralTopic,
    pub allowed_rpc_transports: BTreeSet<RpcTransport>,
    pub retention_class: RetentionClass,
    pub routing_defaults: RoutingDefaults,
}

// String discriminants keep unknown remote variants recoverable rather than
// treating serde enum failures as local programming invariants.
#[derive(Serialize, Deserialize)]
struct DescriptorWire {
    protocol: String,
    pool_id: String,
    authority_group: [u8; 32],
    active_task_part: PartKey,
    register_scope: Vec<u8>,
    register_incarnation: [u8; 32],
    allowed_key_refs: BTreeSet<Url>,
    publication_key: PinnedJwk,
    archive_part: Option<PartKey>,
    router_slot: ObjKey,
    router_heartbeat_topic: [u8; 32],
    allowed_rpc_transports: BTreeSet<String>,
    retention_class: String,
    routing_defaults: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PoolMetadataRejection {
    #[error("unsupported descriptor protocol: {0}")]
    UnsupportedProtocol(String),
    #[error("unsupported pool RPC transport: {0}")]
    UnsupportedTransport(String),
    #[error("unsupported retention class: {0}")]
    UnsupportedRetentionClass(String),
    #[error("unsupported routing defaults: {0}")]
    UnsupportedRoutingDefaults(String),
    #[error("malformed pool metadata: {0}")]
    Malformed(String),
    #[error("descriptor names another pool")]
    WrongPool,
    #[error("descriptor names another authority group")]
    WrongAuthority,
    #[error("current document/group authority does not permit Read")]
    Unauthorized,
}

impl TaskPoolDescriptor {
    fn validate_storage(&self, document: &DocumentId) -> Result<(), PoolMetadataRejection> {
        RegisterBinding::validate_keys(
            document,
            &self.register_scope,
            &self.allowed_key_refs,
            &self.publication_key,
        )
        .map_err(|error| malformed(error.to_string()))
    }
    fn validate_provisioned_keys(&self) -> Result<(), PoolMetadataRejection> {
        let reference = daybook_types::url::parse_facet_ref(&self.publication_key.key_ref)
            .map_err(|error| malformed(error.to_string()))?;
        let document = reference
            .doc_id
            .as_str()
            .parse::<DocumentId>()
            .map_err(|error| malformed(error.to_string()))?;
        self.validate_storage(&document)
    }

    pub(crate) fn encode(&self) -> Result<String, PoolMetadataRejection> {
        self.validate_provisioned_keys()?;
        validate_keys(
            &self.active_task_part,
            self.archive_part.as_ref(),
            &self.router_slot,
        )?;
        if self.allowed_rpc_transports.is_empty() {
            return Err(malformed("empty RPC transport set"));
        }
        let wire = DescriptorWire {
            protocol: TASK_POOL_DESCRIPTOR_PROTOCOL.into(),
            pool_id: self.pool_id.to_string(),
            authority_group: self.authority_group,
            active_task_part: self.active_task_part.clone(),
            register_scope: self.register_scope.clone(),
            register_incarnation: self.register_incarnation,
            allowed_key_refs: self.allowed_key_refs.clone(),
            publication_key: self.publication_key.clone(),
            archive_part: self.archive_part.clone(),
            router_slot: self.router_slot.clone(),
            router_heartbeat_topic: *self.router_heartbeat_topic.as_bytes(),
            allowed_rpc_transports: self
                .allowed_rpc_transports
                .iter()
                .map(|transport| match transport {
                    RpcTransport::IrpcIroh => "irpc-iroh".to_owned(),
                })
                .collect(),
            retention_class: match self.retention_class {
                RetentionClass::UntilAuthoritativeRemoval => "until-authoritative-removal".into(),
            },
            routing_defaults: match self.routing_defaults {
                RoutingDefaults::SharedElection => "shared-election".into(),
            },
        };
        serde_json::to_string(&wire).map_err(|error| malformed(error.to_string()))
    }

    fn decode(json: &str) -> Result<Self, PoolMetadataRejection> {
        let wire: DescriptorWire =
            serde_json::from_str(json).map_err(|error| malformed(error.to_string()))?;
        if wire.protocol != TASK_POOL_DESCRIPTOR_PROTOCOL {
            return Err(PoolMetadataRejection::UnsupportedProtocol(wire.protocol));
        }
        for transport in &wire.allowed_rpc_transports {
            if transport != "irpc-iroh" {
                return Err(PoolMetadataRejection::UnsupportedTransport(
                    transport.clone(),
                ));
            }
        }
        if wire.allowed_rpc_transports.is_empty() {
            return Err(malformed("empty RPC transport set"));
        }
        if wire.retention_class != "until-authoritative-removal" {
            return Err(PoolMetadataRejection::UnsupportedRetentionClass(
                wire.retention_class,
            ));
        }
        if wire.routing_defaults != "shared-election" {
            return Err(PoolMetadataRejection::UnsupportedRoutingDefaults(
                wire.routing_defaults,
            ));
        }
        validate_keys(
            &wire.active_task_part,
            wire.archive_part.as_ref(),
            &wire.router_slot,
        )?;
        let descriptor = Self {
            pool_id: TaskPoolId::from_label(wire.pool_id),
            authority_group: wire.authority_group,
            active_task_part: wire.active_task_part,
            register_scope: wire.register_scope,
            register_incarnation: wire.register_incarnation,
            allowed_key_refs: wire.allowed_key_refs,
            publication_key: wire.publication_key,
            archive_part: wire.archive_part,
            router_slot: wire.router_slot,
            router_heartbeat_topic: BigEphemeralTopic::new(wire.router_heartbeat_topic),
            allowed_rpc_transports: BTreeSet::from([RpcTransport::IrpcIroh]),
            retention_class: RetentionClass::UntilAuthoritativeRemoval,
            routing_defaults: RoutingDefaults::SharedElection,
        };
        descriptor.validate_provisioned_keys()?;
        Ok(descriptor)
    }
}

fn malformed(reason: impl Into<String>) -> PoolMetadataRejection {
    PoolMetadataRejection::Malformed(reason.into())
}

fn validate_keys(
    active: &PartKey,
    archive: Option<&PartKey>,
    slot: &ObjKey,
) -> Result<(), PoolMetadataRejection> {
    // BigSync part/object keys are variable-width byte strings, not necessarily digests.
    if active.as_bytes().is_empty()
        || archive.is_some_and(|part| part.as_bytes().is_empty())
        || slot.as_bytes().is_empty()
    {
        return Err(malformed("empty rendezvous key"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolReference {
    pub document: DocumentId,
    pub authority_group: [u8; 32],
}

impl PoolReference {
    fn key(&self) -> Result<String, PoolMetadataRejection> {
        self.document
            .to_bytes32()
            .map_err(|error| malformed(error.to_string()))?;
        Ok(format!(
            "{}/{}",
            utils_rs::hash::encode_base58_multibase(self.authority_group),
            self.document
        ))
    }

    fn from_key(key: &str) -> Result<Self, PoolMetadataRejection> {
        let (group, document) = key
            .split_once('/')
            .ok_or_else(|| malformed("invalid directory reference"))?;
        let group: ObjKey = group
            .parse()
            .map_err(|error| malformed(format!("invalid group: {error}")))?;
        let document: DocumentId = document
            .parse()
            .map_err(|error| malformed(format!("invalid document: {error}")))?;
        let reference = Self {
            authority_group: group
                .to_bytes32()
                .map_err(|error| malformed(error.to_string()))?,
            document,
        };
        reference
            .document
            .to_bytes32()
            .map_err(|error| malformed(error.to_string()))?;
        Ok(reference)
    }
}

/// Domain-owned lookup across all authority groups. A hint is not a Read grant
/// and does not start a pool; the owner separately loads the actual descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolBindingLookup {
    Absent,
    Pending { document: DocumentId },
    Hint(PoolReference),
    Conflict { references: BTreeSet<PoolReference> },
    Rejected(PoolMetadataRejection),
}

/// Ready is a snapshot authorized at final Read admission, not an ongoing grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolDescriptorSnapshot {
    pub reference: PoolReference,
    pub heads: Vec<automerge::ChangeHash>,
    pub descriptor: TaskPoolDescriptor,
}

impl PoolDescriptorSnapshot {
    /// Construct the register binding without inferring transport or key identity.
    /// The snapshot is metadata, not an ongoing authority grant.
    pub async fn register_binding(&self, repo: &big_repo::BigRepo) -> Res<RegisterBinding> {
        self.register_binding_for_access(repo, big_repo::keyhive_core::access::Access::Read)
            .await
    }

    /// Opaque transport owners receive explicit provisioned metadata without
    /// discovering/decrypting the descriptor. Relay admission never grants Read.
    pub(crate) async fn register_binding_for_access(
        &self,
        repo: &big_repo::BigRepo,
        required: big_repo::keyhive_core::access::Access,
    ) -> Res<RegisterBinding> {
        ensure!(
            self.reference.authority_group == self.descriptor.authority_group,
            "pool authority mismatch"
        );
        self.descriptor.validate_storage(&self.reference.document)?;
        validate_keys(
            &self.descriptor.active_task_part,
            self.descriptor.archive_part.as_ref(),
            &self.descriptor.router_slot,
        )?;
        let sampled = repo
            .coordination_authority(
                self.reference.document.clone(),
                self.reference.authority_group,
            )
            .await?;
        let authority = repo.admit_coordination_access(&sampled, required).await?;
        Ok(RegisterBinding {
            scope: self.descriptor.register_scope.clone(),
            part: self.descriptor.active_task_part.clone(),
            authority,
            allowed_key_refs: self.descriptor.allowed_key_refs.clone(),
            publication_key: self.descriptor.publication_key.clone(),
            incarnation: self.descriptor.register_incarnation,
        })
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "Ready owns the descriptor snapshot inline to avoid a per-query heap allocation"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolDiscovery {
    Absent,
    /// Known lookup locus or reference whose bytes/keys are not available locally.
    Pending {
        documents: BTreeSet<DocumentId>,
    },
    Ready(PoolDescriptorSnapshot),
    Conflict {
        references: BTreeSet<PoolReference>,
    },
    Rejected(PoolMetadataRejection),
}

#[derive(Debug, thiserror::Error)]
pub enum PoolRegistrationError {
    #[error(transparent)]
    Metadata(#[from] PoolMetadataRejection),
    #[error(transparent)]
    Authority(#[from] CoordinationError),
    #[error("pool document is unavailable locally: {0}")]
    Pending(DocumentId),
    #[error("pool has conflicting descriptor bindings")]
    Conflict,
    #[error(transparent)]
    Other(#[from] eyre::Report),
}

/// Stateless handle over an existing durable rendezvous document.
#[derive(Clone)]
pub struct PoolRepo {
    big_repo: SharedBigRepo,
    rendezvous: DocumentId,
    actor: ActorId,
}

impl PoolRepo {
    pub fn new(big_repo: SharedBigRepo, rendezvous: DocumentId, actor: ActorId) -> Self {
        Self {
            big_repo,
            rendezvous,
            actor,
        }
    }

    /// Publish descriptor first, then an independent, non-authoritative lookup hint.
    /// Repeating a completed registration is idempotent; a failure between commits
    /// may leave an unadvertised descriptor. No cross-document atomicity is claimed.
    pub async fn register(
        &self,
        document: DocumentId,
        descriptor: TaskPoolDescriptor,
    ) -> Result<PoolReference, PoolRegistrationError> {
        let reference = PoolReference {
            document,
            authority_group: descriptor.authority_group,
        };
        descriptor.validate_storage(&reference.document)?;
        let key = reference.key()?;
        let json = descriptor.encode()?;
        let directory = self.ready_handle(&self.rendezvous).await?;
        let references = directory
            .with_document_read(|doc| read_directory(doc, &descriptor.pool_id))
            .await?;
        if references.iter().any(|existing| {
            existing.authority_group == reference.authority_group && existing != &reference
        }) {
            return Err(PoolRegistrationError::Conflict);
        }
        let handle = self.ready_handle(&reference.document).await?;
        let existing = handle
            .with_document_read(|doc| read_descriptor(doc, &descriptor.pool_id))
            .await?;
        if existing.len() > 1 {
            return Err(PoolRegistrationError::Conflict);
        }
        if let Some(existing) = existing.first()
            && existing.authority_group != descriptor.authority_group
        {
            return Err(PoolMetadataRejection::WrongAuthority.into());
        }
        let authority = self
            .big_repo
            .coordination_authority(reference.document.clone(), reference.authority_group)
            .await?;
        // Exact descriptor and directory reference are prepared before this final
        // checked admission. This is the accepted-operation linearization: later
        // group revocation does not undo it. The token is neither retained nor reused;
        // ordinary document commit still enforces document Edit and durability.
        self.big_repo.admit_coordination(&authority).await?;
        handle
            .with_document(|doc| {
                if read_descriptor(doc, &descriptor.pool_id)? != existing {
                    return Err(ferr!("pool descriptor changed before admitted mutation"));
                }
                write_descriptor(doc, &self.actor, &descriptor.pool_id, &json)
            })
            .await??;
        // This separate normal document Edit publishes only a lookup hint, not a grant.
        directory
            .with_document(|doc| write_reference(doc, &self.actor, &descriptor.pool_id, &key))
            .await??;
        Ok(reference)
    }

    async fn ready_handle(
        &self,
        document: &DocumentId,
    ) -> Result<BigDocHandle, PoolRegistrationError> {
        match self.big_repo.get_doc(document).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => Ok(handle),
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                Err(PoolRegistrationError::Pending(document.clone()))
            }
        }
    }

    /// Find the sole explicit binding for a managed processor pool. A domain
    /// supplies its existing config document as rendezvous; this never scans
    /// source documents, infers a group, creates a descriptor or starts roles.
    /// Even a sole hint must pass `load_descriptor`'s current Read admission.
    pub async fn lookup_binding(&self, pool: &TaskPoolId) -> Res<PoolBindingLookup> {
        let directory = match self.big_repo.get_doc(&self.rendezvous).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                return Ok(PoolBindingLookup::Pending {
                    document: self.rendezvous.clone(),
                });
            }
        };
        let references = match directory
            .with_document_read(|doc| read_directory(doc, pool))
            .await
        {
            Ok(references) => references,
            Err(error) => return Ok(PoolBindingLookup::Rejected(error)),
        };
        match references.len() {
            0 => Ok(PoolBindingLookup::Absent),
            1 => Ok(PoolBindingLookup::Hint(
                references
                    .into_iter()
                    .next()
                    .expect("one binding reference"),
            )),
            _ => Ok(PoolBindingLookup::Conflict { references }),
        }
    }

    pub async fn discover(&self, pool: &TaskPoolId, group: [u8; 32]) -> Res<PoolDiscovery> {
        let (status, _) = self.discover_with_references(pool, group).await?;
        Ok(status)
    }

    async fn discover_with_references(
        &self,
        pool: &TaskPoolId,
        group: [u8; 32],
    ) -> Res<(PoolDiscovery, BTreeSet<PoolReference>)> {
        let directory = match self.big_repo.get_doc(&self.rendezvous).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                return Ok((
                    PoolDiscovery::Pending {
                        documents: BTreeSet::from([self.rendezvous.clone()]),
                    },
                    BTreeSet::new(),
                ));
            }
        };
        let references = match directory
            .with_document_read(|doc| read_directory(doc, pool))
            .await
        {
            Ok(references) => references,
            Err(error) => return Ok((PoolDiscovery::Rejected(error), BTreeSet::new())),
        };
        let matching: BTreeSet<_> = references
            .iter()
            .filter(|reference| reference.authority_group == group)
            .cloned()
            .collect();
        let status = match matching.len() {
            0 if references.is_empty() => PoolDiscovery::Absent,
            0 => PoolDiscovery::Rejected(PoolMetadataRejection::WrongAuthority),
            1 => {
                self.load_descriptor(
                    matching.first().expect("one matching reference"),
                    pool,
                    group,
                )
                .await?
            }
            _ => PoolDiscovery::Conflict {
                references: matching,
            },
        };
        Ok((status, references))
    }

    pub async fn load_descriptor(
        &self,
        reference: &PoolReference,
        pool: &TaskPoolId,
        group: [u8; 32],
    ) -> Res<PoolDiscovery> {
        if reference.authority_group != group {
            return Ok(PoolDiscovery::Rejected(
                PoolMetadataRejection::WrongAuthority,
            ));
        }
        let handle = match self.big_repo.get_doc(&reference.document).await? {
            DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            DocLookup::Ready(_) | DocLookup::PendingMaterialization | DocLookup::Missing => {
                return Ok(PoolDiscovery::Pending {
                    documents: BTreeSet::from([reference.document.clone()]),
                });
            }
        };
        // Capture private metadata first. All graph reads/awaits precede the final
        // Read admission; document Read alone does not prove named-group Read.
        let (values, heads) = handle
            .with_document_read(|doc| (read_descriptor(doc, pool), doc.get_heads()))
            .await;
        let authority = match self
            .big_repo
            .coordination_authority(reference.document.clone(), group)
            .await
        {
            Ok(authority) => authority,
            Err(error) => return authority_status(error, &reference.document),
        };
        if let Err(error) = self.big_repo.admit_coordination_read(&authority).await {
            return authority_status(error, &reference.document);
        }
        let values = match values {
            Ok(values) => values,
            Err(error) => return Ok(PoolDiscovery::Rejected(error)),
        };
        let mut values = values.into_iter();
        match (values.next(), values.next()) {
            (None, _) => Ok(PoolDiscovery::Rejected(malformed(
                "referenced descriptor is absent",
            ))),
            (Some(_), Some(_)) => Ok(PoolDiscovery::Conflict {
                references: BTreeSet::from([reference.clone()]),
            }),
            (Some(descriptor), None) if descriptor.authority_group != group => Ok(
                PoolDiscovery::Rejected(PoolMetadataRejection::WrongAuthority),
            ),
            (Some(descriptor), None) => match descriptor.validate_storage(&reference.document) {
                Err(error) => Ok(PoolDiscovery::Rejected(error)),
                Ok(()) => Ok(PoolDiscovery::Ready(PoolDescriptorSnapshot {
                    reference: reference.clone(),
                    heads,
                    descriptor,
                })),
            },
        }
    }

    /// Subscribes before querying. The owned watch has no background task.
    pub async fn watch(&self, pool: TaskPoolId, group: [u8; 32]) -> Res<PoolWatch> {
        let (directory_ticket, directory_rx) = self
            .big_repo
            .subscribe_change_listener(big_repo::BigRepoChangeFilter {
                doc_id: Some(big_repo::BigRepoDocIdFilter::new(self.rendezvous.clone())),
                origin: None,
                path: vec![DIRECTORY.into()],
            })
            .await?;
        // Subscribe to the descriptor property before discovering document identities.
        // This closes the new-reference race without a task or per-document watcher pool.
        let (descriptor_ticket, descriptor_rx) = self
            .big_repo
            .subscribe_change_listener(big_repo::BigRepoChangeFilter {
                doc_id: None,
                origin: None,
                path: vec![DESCRIPTORS.into()],
            })
            .await?;
        let (local_ticket, local_rx) = self
            .big_repo
            .subscribe_local_listener(big_repo::BigRepoLocalFilter { doc_id: None })
            .await?;
        let (domain_ticket, domain_rx) = self
            .big_repo
            .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
            .await?;
        let (initial, references) = self.discover_with_references(&pool, group).await?;
        Ok(PoolWatch {
            repo: self.clone(),
            pool,
            group,
            initial,
            references,
            _directory_ticket: directory_ticket,
            _descriptor_ticket: descriptor_ticket,
            _local_ticket: local_ticket,
            _domain_ticket: domain_ticket,
            directory_rx,
            descriptor_rx,
            local_rx,
            domain_rx,
        })
    }
}

fn authority_status(error: CoordinationError, document: &DocumentId) -> Res<PoolDiscovery> {
    match error {
        CoordinationError::Pending => Ok(PoolDiscovery::Pending {
            documents: BTreeSet::from([document.clone()]),
        }),
        CoordinationError::Unauthorized => {
            Ok(PoolDiscovery::Rejected(PoolMetadataRejection::Unauthorized))
        }
        CoordinationError::Invalid(reason) => Ok(PoolDiscovery::Rejected(malformed(reason))),
        CoordinationError::Other(error) => Err(error),
    }
}

/// All registrations are dropped with the watch. Consumers own cancellation and
/// must drop their watches before stopping BigRepo (reverse construction order).
pub struct PoolWatch {
    repo: PoolRepo,
    pool: TaskPoolId,
    group: [u8; 32],
    initial: PoolDiscovery,
    references: BTreeSet<PoolReference>,
    _directory_ticket: big_repo::BigRepoChangeListenerRegistration,
    _descriptor_ticket: big_repo::BigRepoChangeListenerRegistration,
    _local_ticket: big_repo::BigRepoLocalListenerRegistration,
    _domain_ticket: big_repo::BigRepoDomainListenerRegistration,
    directory_rx: mpsc::UnboundedReceiver<Vec<big_repo::BigRepoChangeNotification>>,
    descriptor_rx: mpsc::UnboundedReceiver<Vec<big_repo::BigRepoChangeNotification>>,
    local_rx: mpsc::UnboundedReceiver<Vec<big_repo::BigRepoLocalNotification>>,
    domain_rx: mpsc::UnboundedReceiver<Vec<big_repo::BigRepoDomainNotification>>,
}

impl PoolWatch {
    pub fn initial(&self) -> &PoolDiscovery {
        &self.initial
    }

    pub async fn refresh(&mut self) -> Res<PoolDiscovery> {
        let (status, references) = self
            .repo
            .discover_with_references(&self.pool, self.group)
            .await?;
        self.references = references;
        Ok(status)
    }

    fn relevant_document(&self, document: &DocumentId) -> bool {
        document == &self.repo.rendezvous
            || self
                .references
                .iter()
                .any(|reference| &reference.document == document)
    }

    /// Notifications are wakeups only; every returned snapshot is freshly loaded
    /// and authenticated. May return the same state for a redundant wakeup.
    pub async fn changed(&mut self) -> Res<PoolDiscovery> {
        loop {
            let relevant = tokio::select! {
                changes = self.directory_rx.recv() => {
                    changes.ok_or_else(|| ferr!("pool directory listener closed"))?;
                    true
                }
                changes = self.descriptor_rx.recv() => {
                    changes.ok_or_else(|| ferr!("pool descriptor listener closed"))?.iter().any(|change| {
                        let document = match change {
                            big_repo::BigRepoChangeNotification::DocCreated { doc_id, .. }
                            | big_repo::BigRepoChangeNotification::DocImported { doc_id, .. }
                            | big_repo::BigRepoChangeNotification::DocChanged { doc_id, .. } => doc_id,
                        };
                        self.relevant_document(document)
                    })
                }
                changes = self.local_rx.recv() => {
                    changes.ok_or_else(|| ferr!("pool materialization listener closed"))?.iter().any(|change| {
                        let document = match change {
                            big_repo::BigRepoLocalNotification::DocCreated { doc_id, .. }
                            | big_repo::BigRepoLocalNotification::DocImported { doc_id, .. }
                            | big_repo::BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. }
                            | big_repo::BigRepoLocalNotification::DocMaterializationPending { doc_id }
                            | big_repo::BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
                        };
                        self.relevant_document(document)
                    })
                }
                changes = self.domain_rx.recv() => {
                    changes.ok_or_else(|| ferr!("pool authority listener closed"))?.iter().any(|change| match change {
                        big_repo::BigRepoDomainNotification::DocumentAddedToGroup { doc_id, .. }
                        | big_repo::BigRepoDomainNotification::DocumentRemovedFromGroup { doc_id, .. }
                        | big_repo::BigRepoDomainNotification::DocumentAccessChanged { doc_id, .. }
                        | big_repo::BigRepoDomainNotification::DocumentAccessRevoked { doc_id, .. }
                        | big_repo::BigRepoDomainNotification::DocumentKeyRotated { doc_id } => self.relevant_document(doc_id),
                        // Group membership notifications name only their direct
                        // target, not every group reached through nested membership.
                        // Any such change can invalidate the named group's Read
                        // intersection even while independent document Read survives.
                        big_repo::BigRepoDomainNotification::MemberAddedToGroup { .. }
                        | big_repo::BigRepoDomainNotification::MemberRemovedFromGroup { .. } => true,
                    })
                }
            };
            if relevant {
                return self.refresh().await;
            }
        }
    }
}

// Inspect every conflicting map object as well as every conflicting scalar.
// Autosurgeon's ordinary winner hydration would lose independent references.
fn maps(
    doc: &impl ReadDoc,
    parent: &automerge::ObjId,
    key: &str,
) -> Result<Vec<automerge::ObjId>, PoolMetadataRejection> {
    doc.get_all(parent, key)
        .map_err(|error| malformed(error.to_string()))?
        .into_iter()
        .map(|(value, id)| match value {
            Value::Object(ObjType::Map) => Ok(id),
            _ => Err(malformed(format!("{key} must be a map"))),
        })
        .collect()
}

fn read_directory(
    doc: &impl ReadDoc,
    pool: &TaskPoolId,
) -> Result<BTreeSet<PoolReference>, PoolMetadataRejection> {
    let mut references = BTreeSet::new();
    for root in maps(doc, &automerge::ROOT, DIRECTORY)? {
        for entries in maps(doc, &root, pool.as_str())? {
            for key in doc.keys(&entries) {
                let values = doc
                    .get_all(&entries, &key)
                    .map_err(|error| malformed(error.to_string()))?;
                if values.iter().any(|(value, _)| !matches!(value, Value::Scalar(value) if value.as_ref() == &ScalarValue::Boolean(true))) {
                    return Err(malformed("invalid directory reference marker"));
                }
                references.insert(PoolReference::from_key(&key)?);
            }
        }
    }
    Ok(references)
}

fn read_descriptor(
    doc: &impl ReadDoc,
    pool: &TaskPoolId,
) -> Result<Vec<TaskPoolDescriptor>, PoolMetadataRejection> {
    let mut descriptors = Vec::new();
    for root in maps(doc, &automerge::ROOT, DESCRIPTORS)? {
        for (value, _) in doc
            .get_all(&root, pool.as_str())
            .map_err(|error| malformed(error.to_string()))?
        {
            let Value::Scalar(value) = value else {
                return Err(malformed("descriptor must be complete JSON"));
            };
            let ScalarValue::Str(json) = value.as_ref() else {
                return Err(malformed("descriptor must be complete JSON"));
            };
            let descriptor = TaskPoolDescriptor::decode(json)?;
            if descriptor.pool_id != *pool {
                return Err(PoolMetadataRejection::WrongPool);
            }
            if !descriptors.contains(&descriptor) {
                descriptors.push(descriptor);
            }
        }
    }
    Ok(descriptors)
}

fn writable_maps(
    tx: &mut automerge::transaction::Transaction<'_>,
    parent: &automerge::ObjId,
    key: &str,
) -> Res<Vec<automerge::ObjId>> {
    let existing = maps(tx, parent, key)?;
    if existing.is_empty() {
        Ok(vec![tx.put_object(parent, key, ObjType::Map)?])
    } else {
        Ok(existing)
    }
}

fn write_descriptor(
    doc: &mut automerge::Automerge,
    actor: &ActorId,
    pool: &TaskPoolId,
    json: &str,
) -> Res<()> {
    // A concurrent edit arriving after preflight cannot be silently overwritten.
    if read_descriptor(doc, pool)?.len() > 1 {
        eyre::bail!("conflicting pool descriptor");
    }
    doc.set_actor(actor.clone());
    let mut tx = doc.transaction();
    for root in writable_maps(&mut tx, &automerge::ROOT, DESCRIPTORS)? {
        let current = tx.get_all(&root, pool.as_str())?;
        if current.iter().all(|(value, _)| matches!(value, Value::Scalar(value) if matches!(value.as_ref(), ScalarValue::Str(current) if current.as_str() == json))) && !current.is_empty() {
            continue;
        }
        tx.put(&root, pool.as_str(), json)?;
    }
    tx.commit();
    Ok(())
}

fn write_reference(
    doc: &mut automerge::Automerge,
    actor: &ActorId,
    pool: &TaskPoolId,
    key: &str,
) -> Res<()> {
    doc.set_actor(actor.clone());
    let mut tx = doc.transaction();
    for root in writable_maps(&mut tx, &automerge::ROOT, DIRECTORY)? {
        for entries in writable_maps(&mut tx, &root, pool.as_str())? {
            if tx.get_all(&entries, key)?.is_empty() {
                tx.put(&entries, key, true)?;
            }
        }
    }
    tx.commit();
    Ok(())
}

#[cfg(test)]
mod tests;
