//! Explicit native processor-domain metadata (ADR 010 §6).
//!
//! Management supplies the document/group reference and provisions its generic
//! JWK facet separately. Lookup never creates a document or a key. This document
//! contains no slots, task history, manifest policy, or node-local settings.

use crate::blobs::encrypt::JwkOct;
use crate::interlude::*;
use crate::tasks::pool::PoolReference;
use crate::tasks::storage::{PinnedJwk, RegisterBinding};
use automerge::{ObjType, ReadDoc, ScalarValue, Value, transaction::Transactable};
use big_repo::{CoordinationError, DocLookup};
use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};
use std::collections::BTreeSet;

const METADATA: &str = "triage_processor_domain";
const PROTOCOL: &str = "daybook/triage-processor-domain/v1";

/// Stable management-selected authority locus, not a discovery hint or a grant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProcessorDomainReference {
    pub document: DocumentId,
    pub authority_group: [u8; 32],
}

/// Exact metadata read, authorized at final native Read admission. RegisterStore
/// checks current authority again when opening and using the register.
pub struct ProcessorDomainSnapshot {
    pub reference: ProcessorDomainReference,
    pub heads: Vec<automerge::ChangeHash>,
    pub processor_full_id: String,
    pub pool: PoolReference,
    pub register_binding: RegisterBinding,
}

impl ProcessorDomainSnapshot {
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
                && binding.part.as_bytes() == identity
                && binding.incarnation == identity
                && binding.authority.document() == &self.reference.document
                && binding.authority.group() == &self.reference.authority_group
                && self.pool.document != self.reference.document,
            "processor metadata is detached from its native register binding"
        );
        RegisterBinding::validate_keys(
            &self.reference.document,
            &binding.scope,
            &binding.allowed_key_refs,
            &binding.publication_key,
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
    processor_full_id: String,
    authority_group: [u8; 32],
    pool_document: String,
    pool_authority_group: [u8; 32],
    slot_key: PinnedJwk,
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
        reference.document.to_bytes32().map_err(malformed)?;
        let document: DocumentId = self.pool_document.parse().map_err(malformed)?;
        document.to_bytes32().map_err(malformed)?;
        if document == reference.document {
            return Err(malformed(
                "slot domain and task pool must use separate documents",
            ));
        }
        RegisterBinding::validate_keys(
            &reference.document,
            &register_identity(processor_full_id),
            &BTreeSet::from([self.slot_key.key_ref.clone()]),
            &self.slot_key,
        )
        .map_err(malformed)?;
        Ok(PoolReference {
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
                scope: identity.to_vec(),
                part: PartKey::new(identity),
                authority,
                allowed_key_refs: BTreeSet::from([metadata.slot_key.key_ref.clone()]),
                publication_key: metadata.slot_key,
                incarnation: identity,
            },
            processor_full_id: metadata.processor_full_id,
        }))
    }

    /// Bind a generic JWK already provisioned in this existing native document.
    /// Exact key heads are retained, never replaced with latest document heads.
    /// Repeating identical metadata is idempotent. Management may change the pool
    /// or key binding, but cannot repurpose a domain for another processor/group.
    pub async fn provision(
        &self,
        reference: &ProcessorDomainReference,
        processor_full_id: String,
        pool: PoolReference,
        slot_key: PinnedJwk,
    ) -> Result<(), ProcessorDomainProvisionError> {
        let metadata = Metadata {
            protocol: PROTOCOL.into(),
            processor_full_id,
            authority_group: reference.authority_group,
            pool_document: pool.document.to_string(),
            pool_authority_group: pool.authority_group,
            slot_key,
        };
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
            .await
            .map_err(|error| provision_authority_error(error, &reference.document))?;
        // Native exact document/group Edit admission linearizes this operation;
        // the ordinary document commit separately enforces Edit and durability.
        self.big_repo
            .admit_coordination(&sampled)
            .await
            .map_err(|error| provision_authority_error(error, &reference.document))?;
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
                tx.put(automerge::ROOT, METADATA, json.as_str())
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
    }
}

fn authority_status(error: CoordinationError, document: &DocumentId) -> Res<ProcessorDomainLoad> {
    match error {
        CoordinationError::Pending => Ok(pending(document)),
        CoordinationError::Unauthorized => Ok(ProcessorDomainLoad::Rejected(
            ProcessorDomainRejection::Unauthorized,
        )),
        CoordinationError::Invalid(reason) => Ok(ProcessorDomainLoad::Rejected(malformed(reason))),
        CoordinationError::Other(error) => Err(error),
    }
}

fn provision_authority_error(
    error: CoordinationError,
    document: &DocumentId,
) -> ProcessorDomainProvisionError {
    match error {
        CoordinationError::Pending => ProcessorDomainProvisionError::Pending(document.clone()),
        error => ProcessorDomainProvisionError::Authority(error),
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

fn read_metadata(doc: &impl ReadDoc) -> Result<Option<Metadata>, ProcessorDomainRejection> {
    let mut metadata = None;
    for (value, _) in doc.get_all(automerge::ROOT, METADATA).map_err(malformed)? {
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
            document: DocumentId::new([3; 32]),
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
        let missing = ProcessorDomainReference {
            document: DocumentId::new([4; 32]),
            authority_group: reference.authority_group,
        };
        assert!(
            matches!(domains.load(&missing, "plug/processor").await?, ProcessorDomainLoad::Pending { document } if document == missing.document)
        );
        assert!(
            matches!(domains.provision(&missing, "plug/processor".into(), pool.clone(), PinnedJwk {
            key_ref: daybook_types::url::build_facet_ref(&missing.document.to_string(), &FacetKey::from(WellKnownFacetTag::Jwk))?,
            heads: key.heads.clone(),
        }).await, Err(ProcessorDomainProvisionError::Pending(document)) if document == missing.document)
        );
        repo.wait_for_quiescence(None).await?;
        domains
            .provision(
                &reference,
                "plug/processor".into(),
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
                .provision(&reference, "plug/other".into(), pool.clone(), key.clone())
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
        let changed_pool = PoolReference {
            document: DocumentId::new([5; 32]),
            authority_group: pool.authority_group,
        };
        domains
            .provision(
                &reference,
                "plug/processor".into(),
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
                    tx.put(automerge::ROOT, METADATA, serde_json::to_string(&metadata)?)?;
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
                .provision(&reference, "plug/processor".into(), pool, key)
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
