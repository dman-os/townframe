//! Durable authenticated current registers. Physical scope retirement is not implemented here.
//!
//! The store owns the checked BigRepo boundary: callers cannot inject writer identities,
//! SQL scopes or reducer admissions. Crypto awaits precede SQLite transactions. Current
//! state and its BigSync publication commit together, so restart needs no publication scan.

use std::sync::Arc;

use big_repo::keyhive_core::access::Access;
use big_repo::{BigRepo, CoordinationAuthority, CoordinationError};
use big_sync::{HostPartStore, LocalPartRevisionReader, SqliteObjWrite, SqlitePartStore};
use big_sync_core::{
    ObjKey, PartKey,
    part_store::CursorIndex,
    rpc::{SubPartsRequest, SubscriptionTarget},
};
use keyhive_crypto::verifiable::Verifiable;
use sqlx::{Sqlite, Transaction};
use utils_rs::prelude::eyre::ensure;
use utils_rs::prelude::*;

use super::control::{RegisterSource, RegisterSourceControl};
use crate::blobs::encrypt::{
    CONTENT_ENCODING_AES128GCM, EncodingParams, JwkOct, MasterKey, decrypt_bytes, encrypt_with_rs,
};
use big_sync_core::encrypted_register::{
    CausalVersion, EncryptedRegister, LaneState, Limits, MergeOutcome, OriginalStatement,
    RegisterKey, RegisterSnapshot, Representation, RepresentationBinding,
};
use daybook_types::doc::{ChangeHashSet, FacetTag, WellKnownFacet, WellKnownFacetTag};
use std::collections::BTreeSet;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinnedJwk {
    pub key_ref: Url,
    pub heads: ChangeHashSet,
}

/// Domain-provisioned scope and allowed key slots; opening never creates a key.
#[derive(Clone)]
pub struct RegisterBinding {
    pub generation: u64,
    pub predecessor_parts: BTreeSet<PartKey>,
    pub historical_key_authorities: std::collections::BTreeMap<String, [u8; 32]>,
    pub scope: Vec<u8>,
    /// Explicit current transport part selected by the owning domain descriptor.
    /// Scope and cryptographic incarnation do not choose subscription identity.
    pub part: PartKey,
    pub authority: CoordinationAuthority,
    pub allowed_key_refs: BTreeSet<Url>,
    pub publication_key: PinnedJwk,
    pub incarnation: [u8; 32],
}

impl RegisterBinding {
    pub(crate) fn validate_keys(
        document: &big_repo::DocumentId,
        scope: &[u8],
        allowed: &BTreeSet<Url>,
        publication: &PinnedJwk,
        history: &std::collections::BTreeMap<String, [u8; 32]>,
    ) -> Res<()> {
        ensure!(
            !scope.is_empty() && scope.len() <= MAX_SLOT_BYTES,
            "register scope exceeds budget"
        );
        let document_id = document.to_string();
        for key_ref in allowed {
            let reference = daybook_types::url::parse_facet_ref(key_ref)?;
            let canonical = daybook_types::url::build_facet_ref(
                reference.doc_id.as_str(),
                &reference.facet_key,
            )?;
            ensure!(
                (reference.doc_id.as_str() == document_id
                    || history.contains_key(reference.doc_id.as_str()))
                    && reference.branch.is_none()
                    && reference.at.is_none()
                    && reference.facet_key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk)
                    && canonical == *key_ref,
                "pool key reference must name a canonical JWK facet in its native authority document"
            );
        }
        ensure!(
            allowed.contains(&publication.key_ref)
                && daybook_types::url::parse_facet_ref(&publication.key_ref)?
                    .doc_id
                    .as_str()
                    == document_id,
            "publication key outside pool key slots"
        );
        ensure!(
            !publication.heads.0.is_empty(),
            "publication key must pin document heads"
        );
        Ok(())
    }
}

const MAX_CIPHERTEXT_BYTES: usize = 1024 * 1024;
const MAX_FRONTIER_ENTRIES: usize = 64;
const MAX_SLOT_BYTES: usize = 4096;

/// One checked authority document/group, in one existing SQLite part-store scope.
/// Incarnation is configured transport identity, not a scope-retirement certificate.
pub struct RegisterStore {
    parts: Arc<SqlitePartStore>,
    repo: Arc<BigRepo>,
    authority: CoordinationAuthority,
    scope: Vec<u8>,
    sources: std::sync::RwLock<RegisterSources>,
    certified_sources: std::sync::RwLock<BTreeSet<Url>>,
    source_admission: tokio::sync::Mutex<()>,
    publication_key: tokio::sync::RwLock<PinnedJwk>,
    incarnation: [u8; 32],
    part: PartKey,
    generation: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct RegisterSources {
    allowed_key_refs: BTreeSet<Url>,
    predecessor_parts: BTreeSet<PartKey>,
    historical_key_authorities: std::collections::BTreeMap<String, [u8; 32]>,
}

// Boxing this short-lived stack value would add an allocation to every write.
#[expect(clippy::large_enum_variant)]
enum Incoming {
    Local(Representation),
    Remote(RegisterSnapshot),
}

impl RegisterStore {
    pub async fn open(
        parts: Arc<SqlitePartStore>,
        repo: Arc<BigRepo>,
        binding: RegisterBinding,
    ) -> Res<Self> {
        Self::open_current(parts, repo, binding, None).await
    }

    // Native transport supplies an already authenticated certificate. Commit
    // that opaque recovery input with selection, before any handoff await.
    pub(crate) async fn open_current(
        parts: Arc<SqlitePartStore>,
        repo: Arc<BigRepo>,
        binding: RegisterBinding,
        control: Option<String>,
    ) -> Res<Self> {
        let RegisterBinding {
            generation,
            predecessor_parts,
            historical_key_authorities,
            scope,
            part,
            authority,
            allowed_key_refs,
            publication_key,
            incarnation,
        } = binding;
        // Native facets avoid Drawer registration's additional admin grants.
        RegisterBinding::validate_keys(
            authority.document(),
            &scope,
            &allowed_key_refs,
            &publication_key,
            &historical_key_authorities,
        )?;
        let sampled = repo
            .coordination_authority(authority.document().clone(), *authority.group())
            .await?;
        let authority = repo
            .admit_coordination_access(&sampled, Access::Relay)
            .await?;
        let store = Self {
            parts,
            repo,
            scope,
            authority,
            certified_sources: Default::default(),
            sources: std::sync::RwLock::new(RegisterSources {
                allowed_key_refs,
                predecessor_parts,
                historical_key_authorities,
            }),
            source_admission: tokio::sync::Mutex::new(()),
            publication_key: tokio::sync::RwLock::new(publication_key),
            incarnation,
            part,
            generation,
        };
        let mut write = store.parts.begin_obj_write(store.control_key()).await?;
        for query in [
            "CREATE TABLE IF NOT EXISTS encrypted_register_source_control (
                register_scope BLOB NOT NULL
              , key_ref TEXT NOT NULL
              , certificate_json TEXT NOT NULL
              , PRIMARY KEY(register_scope, key_ref)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_current_control (
                register_scope BLOB PRIMARY KEY
              , certificate_json TEXT NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_sources (
                register_scope BLOB PRIMARY KEY
              , sources_json TEXT NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_rotation (
                register_scope BLOB PRIMARY KEY
              , generation BLOB NOT NULL
              , part_id BLOB NOT NULL
              , authority_document TEXT NOT NULL
              , authority_group BLOB NOT NULL
              , incarnation BLOB NOT NULL
              , replay_after INTEGER NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_handoff (
                register_scope BLOB NOT NULL
              , destination_part BLOB NOT NULL
              , source_part BLOB NOT NULL
              , through_revision INTEGER NOT NULL
              , PRIMARY KEY(register_scope, destination_part, source_part)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_current (
                scope_id INTEGER NOT NULL
              , logical_key BLOB NOT NULL
              , snapshot_json TEXT NOT NULL
              , PRIMARY KEY(scope_id, logical_key)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_sequence (
                register_scope BLOB NOT NULL
              , writer BLOB NOT NULL
              , next_sequence INTEGER NOT NULL
              , PRIMARY KEY(register_scope, writer)
            )",
            "CREATE TABLE IF NOT EXISTS encrypted_register_consumer_v2 (
                scope_id INTEGER NOT NULL
              , consumer BLOB NOT NULL
              , selector BLOB NOT NULL
              , through_revision INTEGER NOT NULL
              , PRIMARY KEY(scope_id, consumer, selector)
            )",
        ] {
            sqlx::query(query)
                .execute(&mut **write.context_mut())
                .await?;
        }
        let retained: Option<String> = sqlx::query_scalar(
            "SELECT sources_json FROM encrypted_register_sources WHERE register_scope = ?",
        )
        .bind(&store.scope)
        .fetch_optional(&mut **write.context_mut())
        .await?;
        if let Some(retained) = retained {
            let retained: RegisterSources = serde_json::from_str(&retained)?;
            let mut sources = store
                .sources
                .write()
                .expect("register source lock poisoned");
            sources.allowed_key_refs.extend(retained.allowed_key_refs);
            sources.predecessor_parts.extend(retained.predecessor_parts);
            for (document, group) in retained.historical_key_authorities {
                if let Some(existing) = sources.historical_key_authorities.insert(document, group) {
                    ensure!(
                        existing == group,
                        "retained source changes native authority"
                    );
                }
            }
        }
        let legacy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'encrypted_register_consumer')")
            .fetch_one(&mut **write.context_mut()).await?;
        if legacy {
            sqlx::query(
                "INSERT INTO encrypted_register_consumer_v2
                 SELECT scope_id
                      , consumer
                      , selector
                      , through_revision
                   FROM encrypted_register_consumer",
            )
            .execute(&mut **write.context_mut())
            .await?;
            sqlx::query("DROP TABLE encrypted_register_consumer")
                .execute(&mut **write.context_mut())
                .await?;
        }
        #[derive(sqlx::FromRow)]
        struct RotationState {
            generation: Vec<u8>,
            part_id: Vec<u8>,
            authority_document: String,
            authority_group: Vec<u8>,
            incarnation: Vec<u8>,
        }
        let previous: Option<RotationState> = sqlx::query_as(
            "SELECT generation
                  , part_id
                  , authority_document
                  , authority_group
                  , incarnation
               FROM encrypted_register_rotation
              WHERE register_scope = ?",
        )
        .bind(&store.scope)
        .fetch_optional(&mut **write.context_mut())
        .await?;
        let generation = store.generation.to_be_bytes();
        let document = store.authority.document().to_string();
        if let Some(previous) = &previous {
            let previous_generation = &previous.generation;
            let part = &previous.part_id;
            let previous_document = &previous.authority_document;
            ensure!(
                previous.incarnation.as_slice() == store.incarnation
                    && previous.authority_group.as_slice() == store.authority.group(),
                "rotation changes logical register identity"
            );
            let previous_document: big_repo::DocumentId = previous_document.parse()?;
            let selected = generation.as_slice() > previous_generation.as_slice()
                || (generation.as_slice() == previous_generation.as_slice()
                    && *store.authority.document() <= previous_document);
            if !selected {
                return Err(CoordinationError::Unauthorized.into());
            }
            if control.is_some() && part.as_slice() != store.part.as_bytes() {
                // This predecessor is our durable admitted owner, not a history
                // assertion from the peer that supplied current control.
                store
                    .sources
                    .write()
                    .expect("register source lock poisoned")
                    .predecessor_parts
                    .insert(PartKey::new(part.clone()));
            }
            ensure!(
                part.as_slice() == store.part.as_bytes()
                    || store.accepts_part(&PartKey::new(part.clone())),
                "successor does not account for predecessor part"
            );
            // Concurrent same-generation descriptors cannot name one another's
            // fresh keys. Retain the previously admitted owner's provenance locally.
            if previous_document != *store.authority.document() {
                store
                    .sources
                    .write()
                    .expect("register source lock poisoned")
                    .historical_key_authorities
                    .insert(previous_document.to_string(), *store.authority.group());
            }
        }
        let replay_after: i64 = if store.generation == 0 {
            0
        } else {
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&mut **write.context_mut())
                .await?
        };
        sqlx::query(
            "INSERT INTO encrypted_register_rotation (
                register_scope
              , generation
              , part_id
              , authority_document
              , authority_group
              , incarnation
              , replay_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(register_scope) DO UPDATE SET
                generation = excluded.generation
              , part_id = excluded.part_id
              , authority_document = excluded.authority_document
              , replay_after = CASE WHEN encrypted_register_rotation.part_id = excluded.part_id
                                    THEN encrypted_register_rotation.replay_after
                                    ELSE excluded.replay_after END",
        )
        .bind(&store.scope)
        .bind(generation.as_slice())
        .bind(store.part.as_bytes())
        .bind(document)
        .bind(store.authority.group().as_slice())
        .bind(store.incarnation.as_slice())
        .bind(replay_after)
        .execute(&mut **write.context_mut())
        .await?;
        let sources =
            serde_json::to_string(&*store.sources.read().expect("register source lock poisoned"))?;
        sqlx::query(
            "INSERT INTO encrypted_register_sources (register_scope, sources_json) VALUES (?, ?)
             ON CONFLICT(register_scope) DO UPDATE SET sources_json = excluded.sources_json",
        )
        .bind(&store.scope)
        .bind(sources)
        .execute(&mut **write.context_mut())
        .await?;
        let previous_control: Option<String> = sqlx::query_scalar("SELECT certificate_json FROM encrypted_register_current_control WHERE register_scope = ?")
            .bind(&store.scope).fetch_optional(&mut **write.context_mut()).await?;
        if let Some(previous_control) = previous_control {
            store
                .retain_source_control(&mut write, &previous_control)
                .await?;
        }
        if let Some(control) = &control {
            store.retain_source_control(&mut write, control).await?;
        }
        if let Some(control) = control {
            sqlx::query("INSERT INTO encrypted_register_current_control (register_scope, certificate_json) VALUES (?, ?) ON CONFLICT(register_scope) DO UPDATE SET certificate_json = excluded.certificate_json")
                .bind(&store.scope).bind(control).execute(&mut **write.context_mut()).await?;
        } else if previous.as_ref().is_none_or(|previous| {
            previous.part_id.as_slice() != store.part.as_bytes()
                || previous.authority_document != store.authority.document().to_string()
        }) {
            sqlx::query("DELETE FROM encrypted_register_current_control WHERE register_scope = ?")
                .bind(&store.scope)
                .execute(&mut **write.context_mut())
                .await?;
        }
        let certified: Vec<String> = sqlx::query_scalar(
            "SELECT key_ref FROM encrypted_register_source_control WHERE register_scope = ?",
        )
        .bind(&store.scope)
        .fetch_all(&mut **write.context_mut())
        .await?;
        let certified = certified
            .into_iter()
            .map(|key| Url::parse(&key))
            .collect::<Result<BTreeSet<_>, _>>()?;
        write.commit().await?;
        *store
            .certified_sources
            .write()
            .expect("register source lock poisoned") = certified;
        store.carry_forward().await?;
        Ok(store)
    }

    pub(crate) async fn remember_current_control(&self, control: String) -> Res<()> {
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        let source_key = self.retain_source_control(&mut write, &control).await?;
        sqlx::query("INSERT INTO encrypted_register_current_control (register_scope, certificate_json) VALUES (?, ?) ON CONFLICT(register_scope) DO UPDATE SET certificate_json = excluded.certificate_json")
            .bind(&self.scope).bind(control).execute(&mut **write.context_mut()).await?;
        write.commit().await?;
        self.certified_sources
            .write()
            .expect("register source lock poisoned")
            .insert(source_key);
        Ok(())
    }

    /// Advertisements must describe the durable owner, not a retained source.
    pub(crate) async fn control_authority(&self) -> Res<CoordinationAuthority> {
        let authority = self.fresh_authority(Access::Relay).await?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        drop(write);
        Ok(self
            .repo
            .admit_coordination_access(&authority, Access::Relay)
            .await?)
    }

    /// Bounded current-part replay; interrupted moves repeat idempotently. Late
    /// predecessor deliveries are routed through this same selected store.
    async fn carry_forward(&self) -> Res<()> {
        use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits};
        let predecessors = self
            .sources
            .read()
            .expect("register source lock poisoned")
            .predecessor_parts
            .clone();
        for part in &predecessors {
            if part == &self.part {
                continue;
            }
            let mut write = self.parts.begin_obj_write(self.control_key()).await?;
            self.check_current(&mut write).await?;
            let after: Option<i64> = sqlx::query_scalar(
                "SELECT through_revision
                   FROM encrypted_register_handoff
                  WHERE register_scope = ? AND destination_part = ? AND source_part = ?",
            )
            .bind(&self.scope)
            .bind(self.part.as_bytes())
            .bind(part.as_bytes())
            .fetch_optional(&mut **write.context_mut())
            .await?;
            write.commit().await?;
            let after = u64::try_from(after.unwrap_or(0))?;
            let mut reader = match self
                .parts
                .open_revision_reader(SubPartsRequest {
                    lower_bound: after,
                    targets: [SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: after,
                    }]
                    .into(),
                })
                .await?
            {
                Ok(reader) => reader,
                // A fresh replica has no local predecessor membership to carry.
                // Authenticated late deliveries still enter the selected store.
                Err(big_sync_core::rpc::ListPartsError::UnkownParts { .. }) => continue,
            };
            loop {
                let (through, complete) = match reader.next(RevisionReadLimits::default()).await? {
                    RevisionRead::ReplayComplete { through } => (through, true),
                    RevisionRead::Entries { revision, entries } => {
                        for event in entries {
                            if let big_sync_core::rpc::PartEvent::Changed(changed) = event {
                                self.receive(
                                    &changed.obj_id,
                                    std::slice::from_ref(&self.part),
                                    changed.payload,
                                )
                                .await?;
                            }
                        }
                        (revision, false)
                    }
                };
                // Publication precedes progress: interrupted batches repeat
                // idempotently rather than skipping retained evidence.
                let mut write = self.parts.begin_obj_write(self.control_key()).await?;
                self.check_current(&mut write).await?;
                sqlx::query(
                    "INSERT INTO encrypted_register_handoff (
                        register_scope
                      , destination_part
                      , source_part
                      , through_revision
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(register_scope, destination_part, source_part)
                    DO UPDATE SET through_revision = MAX(through_revision, excluded.through_revision)",
                )
                .bind(&self.scope)
                .bind(self.part.as_bytes())
                .bind(part.as_bytes())
                .bind(i64::try_from(through)?)
                .execute(&mut **write.context_mut())
                .await?;
                write.commit().await?;
                if complete {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Admit a native predecessor or losing proposal without replacing the
    /// deterministic selected owner or rewriting its original signed statements.
    pub(crate) async fn adopt_source(&self, binding: &RegisterBinding) -> Res<()> {
        ensure!(
            binding.scope == self.scope
                && binding.incarnation == self.incarnation
                && binding.authority.group() == self.authority.group()
                && binding.generation <= self.generation,
            "source does not belong to selected logical register lineage"
        );
        RegisterBinding::validate_keys(
            binding.authority.document(),
            &binding.scope,
            &binding.allowed_key_refs,
            &binding.publication_key,
            &binding.historical_key_authorities,
        )?;
        let sampled = self
            .repo
            .coordination_authority(
                binding.authority.document().clone(),
                *binding.authority.group(),
            )
            .await?;
        self.repo
            .admit_coordination_access(&sampled, Access::Relay)
            .await?;
        self.fresh_authority(Access::Relay).await?;
        let _admission = self.source_admission.lock().await;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        let mut sources = self
            .sources
            .read()
            .expect("register source lock poisoned")
            .clone();
        sources
            .allowed_key_refs
            .extend(binding.allowed_key_refs.iter().cloned());
        sources
            .predecessor_parts
            .extend(binding.predecessor_parts.iter().cloned());
        sources.predecessor_parts.insert(binding.part.clone());
        for (document, group) in binding
            .historical_key_authorities
            .iter()
            .map(|(doc, group)| (doc.clone(), *group))
            .chain(std::iter::once((
                binding.authority.document().to_string(),
                *binding.authority.group(),
            )))
        {
            if let Some(existing) = sources.historical_key_authorities.insert(document, group) {
                ensure!(
                    existing == group,
                    "source changes retained native authority"
                );
            }
        }
        let encoded = serde_json::to_string(&sources)?;
        sqlx::query(
            "UPDATE encrypted_register_sources SET sources_json = ? WHERE register_scope = ?",
        )
        .bind(encoded)
        .bind(&self.scope)
        .execute(&mut **write.context_mut())
        .await?;
        write.commit().await?;
        *self.sources.write().expect("register source lock poisoned") = sources;
        self.carry_forward().await
    }

    pub(crate) fn authority_document(&self) -> &big_repo::DocumentId {
        self.authority.document()
    }

    async fn retain_source_control(
        &self,
        write: &mut SqliteObjWrite<'_>,
        encoded: &str,
    ) -> Res<Url> {
        let control: RegisterSourceControl = serde_json::from_str(encoded)?;
        let (_, source) = control.decode(*self.authority.group())?;
        ensure!(
            source.scope == self.scope && source.incarnation == self.incarnation,
            "source certificate changes register identity"
        );
        sqlx::query("INSERT INTO encrypted_register_source_control (register_scope, key_ref, certificate_json) VALUES (?, ?, ?) ON CONFLICT(register_scope, key_ref) DO UPDATE SET certificate_json = excluded.certificate_json")
            .bind(&self.scope).bind(source.key_ref.as_str()).bind(encoded).execute(&mut **write.context_mut()).await?;
        Ok(source.key_ref)
    }

    pub(crate) fn missing_sources(&self, incoming: &RegisterSnapshot) -> Res<BTreeSet<Url>> {
        let sources = self.sources.read().expect("register source lock poisoned");
        let certified = self
            .certified_sources
            .read()
            .expect("register source lock poisoned");
        ensure!(
            incoming.lanes.len() <= MAX_FRONTIER_ENTRIES,
            "register lane count exceeds budget"
        );
        let mut missing = BTreeSet::new();
        let current_document = self.authority.document().to_string();
        for lane in incoming.lanes.values() {
            if let LaneState::Current { representation } = lane {
                ensure!(
                    representation.ciphertext.len() <= MAX_CIPHERTEXT_BYTES
                        && representation.binding.key_heads.len() <= MAX_FRONTIER_ENTRIES
                        && representation.binding.key_ref.len() <= 16 * 1024,
                    "register representation exceeds source admission budget"
                );
                let key_ref = Url::parse(std::str::from_utf8(&representation.binding.key_ref)?)?;
                let historical = daybook_types::url::parse_facet_ref(&key_ref)?
                    .doc_id
                    .as_str()
                    != current_document;
                if !sources.allowed_key_refs.contains(&key_ref)
                    || (historical && !certified.contains(&key_ref))
                {
                    missing.insert(key_ref);
                }
            }
        }
        Ok(missing)
    }

    pub(crate) async fn source_control(
        &self,
        peer: &big_sync_core::PeerKey,
        document: &big_repo::DocumentId,
        key_ref: &Url,
    ) -> Res<RegisterSourceControl> {
        let authority = self.control_authority().await?;
        if document != authority.document() {
            return Err(CoordinationError::Pending.into());
        }
        if !authority
            .effective_agents()
            .iter()
            .any(|(agent, access)| agent.as_slice() == peer.as_bytes() && *access >= Access::Relay)
        {
            return Err(CoordinationError::Unauthorized.into());
        }
        if !self
            .sources
            .read()
            .expect("register source lock poisoned")
            .allowed_key_refs
            .contains(key_ref)
        {
            return Err(CoordinationError::Invalid("unknown register source").into());
        }
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        let cached: Option<String> = sqlx::query_scalar("SELECT certificate_json FROM encrypted_register_source_control WHERE register_scope = ? AND key_ref = ?")
            .bind(&self.scope).bind(key_ref.as_str()).fetch_optional(&mut **write.context_mut()).await?;
        drop(write);
        if let Some(encoded) = cached {
            let control: RegisterSourceControl = serde_json::from_str(&encoded)?;
            match control
                .admit(
                    &self.repo,
                    *authority.group(),
                    Some(peer),
                    (&self.scope, &self.incarnation, key_ref),
                )
                .await
            {
                Ok(_) => {
                    self.control_authority().await?;
                    return Ok(control);
                }
                Err(error)
                    if matches!(
                        error.downcast_ref::<CoordinationError>(),
                        Some(CoordinationError::Unauthorized)
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        let reference = daybook_types::url::parse_facet_ref(key_ref)?;
        let historical = self
            .repo
            .coordination_authority(reference.doc_id.as_str().parse()?, *authority.group())
            .await?;
        self.repo
            .admit_coordination_access(&historical, Access::Relay)
            .await?;
        let authority = self.fresh_authority(Access::Edit).await?;
        let source = RegisterSource {
            scope: self.scope.clone(),
            incarnation: self.incarnation,
            key_ref: key_ref.clone(),
        };
        let control = RegisterSourceControl::sign(&self.repo, &authority, peer, &source).await?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        self.retain_source_control(&mut write, &serde_json::to_string(&control)?)
            .await?;
        write.commit().await?;
        self.certified_sources
            .write()
            .expect("register source lock poisoned")
            .insert(key_ref.clone());
        Ok(control)
    }

    pub(crate) async fn import_source_control(
        &self,
        control: &RegisterSourceControl,
        key_ref: &Url,
        peer: &big_sync_core::PeerKey,
    ) -> Res<()> {
        let source = control
            .admit(
                &self.repo,
                *self.authority.group(),
                Some(peer),
                (&self.scope, &self.incarnation, key_ref),
            )
            .await?;
        self.fresh_authority(Access::Relay).await?;
        let _admission = self.source_admission.lock().await;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        let mut sources = self
            .sources
            .read()
            .expect("register source lock poisoned")
            .clone();
        sources.allowed_key_refs.insert(source.key_ref.clone());
        let reference = daybook_types::url::parse_facet_ref(&source.key_ref)?;
        sources
            .historical_key_authorities
            .insert(reference.doc_id.to_string(), *self.authority.group());
        self.retain_source_control(&mut write, &serde_json::to_string(control)?)
            .await?;
        sqlx::query(
            "UPDATE encrypted_register_sources SET sources_json = ? WHERE register_scope = ?",
        )
        .bind(serde_json::to_string(&sources)?)
        .bind(&self.scope)
        .execute(&mut **write.context_mut())
        .await?;
        write.commit().await?;
        self.certified_sources
            .write()
            .expect("register source lock poisoned")
            .insert(key_ref.clone());
        *self.sources.write().expect("register source lock poisoned") = sources;
        Ok(())
    }

    fn control_key(&self) -> ObjKey {
        ObjKey::new(self.part.as_bytes())
    }

    /// Refresh provisioned key metadata without splitting the retained owner.
    /// Opaque relays retain key references/heads, not key bytes. Publication and
    /// plaintext release independently resolve keys under their Read/Edit gates.
    pub(crate) async fn refresh_publication_key(&self, binding: &RegisterBinding) -> Res<()> {
        ensure!(
            binding.scope == self.scope
                && binding.part == self.part
                && binding.incarnation == self.incarnation
                && binding.authority.document() == self.authority.document()
                && binding.authority.group() == self.authority.group()
                && binding.allowed_key_refs.is_subset(
                    &self
                        .sources
                        .read()
                        .expect("register source lock poisoned")
                        .allowed_key_refs
                ),
            "register authority, transport, or key slots changed; explicit migration is required"
        );
        self.fresh_authority(Access::Relay).await?;
        let mut publication = self.publication_key.write().await;
        if *publication != binding.publication_key {
            publication.clone_from(&binding.publication_key);
        }
        Ok(())
    }
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub fn part(&self) -> &PartKey {
        &self.part
    }

    pub(crate) fn for_each_transport_part(&self, mut visit: impl FnMut(&PartKey)) {
        visit(&self.part);
        let sources = self.sources.read().expect("register source lock poisoned");
        for part in &sources.predecessor_parts {
            if part != &self.part {
                visit(part);
            }
        }
    }

    pub(crate) fn transport_parts(&self) -> Vec<PartKey> {
        let sources = self.sources.read().expect("register source lock poisoned");
        let mut parts = Vec::with_capacity(1 + sources.predecessor_parts.len());
        parts.push(self.part.clone());
        parts.extend(
            sources
                .predecessor_parts
                .iter()
                .filter(|part| *part != &self.part)
                .cloned(),
        );
        parts
    }

    pub(crate) fn accepts_part(&self, part: &PartKey) -> bool {
        part == &self.part
            || self
                .sources
                .read()
                .expect("register source lock poisoned")
                .predecessor_parts
                .contains(part)
    }

    fn source_group(&self, document: &str) -> Option<[u8; 32]> {
        self.sources
            .read()
            .expect("register source lock poisoned")
            .historical_key_authorities
            .get(document)
            .copied()
    }

    pub(crate) fn scope(&self) -> &[u8] {
        &self.scope
    }

    pub(crate) fn recognizes_authority(
        &self,
        document: &big_repo::DocumentId,
        group: &[u8; 32],
    ) -> bool {
        (document == self.authority.document() && group == self.authority.group())
            || self.source_group(&document.to_string()).as_ref() == Some(group)
    }

    pub fn key(&self, slot: &[u8]) -> Res<RegisterKey> {
        ensure!(
            slot.len() <= MAX_SLOT_BYTES,
            "coordination slot exceeds budget"
        );
        Ok(RegisterKey {
            scope: self.scope.clone(),
            slot: slot.to_vec(),
        })
    }

    fn limits(&self, key: RegisterKey) -> Limits {
        Limits {
            key,
            max_key_bytes: MAX_SLOT_BYTES,
            max_ciphertext_bytes: MAX_CIPHERTEXT_BYTES,
            max_frontier_entries: MAX_FRONTIER_ENTRIES,
            max_metadata_bytes: 16 * 1024,
        }
    }

    async fn resolve_key(&self, pinned: &PinnedJwk) -> Res<MasterKey> {
        ensure!(
            self.sources
                .read()
                .expect("register source lock poisoned")
                .allowed_key_refs
                .contains(&pinned.key_ref),
            "key reference outside configured domain slots"
        );
        ensure!(
            !pinned.heads.0.is_empty(),
            "key reference must pin document heads"
        );
        let reference = daybook_types::url::parse_facet_ref(&pinned.key_ref)?;
        ensure!(
            reference.facet_key.tag == FacetTag::WellKnown(WellKnownFacetTag::Jwk),
            "key reference must name a JWK facet"
        );
        ensure!(
            reference.at.is_none(),
            "key heads belong in the explicit pinned-heads field"
        );
        let document: big_repo::DocumentId = reference.doc_id.as_str().parse()?;
        if document != *self.authority.document() {
            let group = self
                .source_group(reference.doc_id.as_str())
                .ok_or_eyre("historical key document has no authenticated authority binding")?;
            let sampled = self
                .repo
                .coordination_authority(document.clone(), group)
                .await?;
            self.repo.admit_coordination_read(&sampled).await?;
        }
        let handle = match self.repo.get_doc(&document).await? {
            big_repo::DocLookup::Ready(handle) if !handle.is_partially_decrypted() => handle,
            big_repo::DocLookup::Ready(_)
            | big_repo::DocLookup::Missing
            | big_repo::DocLookup::PendingMaterialization => {
                // Signed control can precede native document materialization.
                // Retain the operation until its actual key bytes arrive.
                return Err(CoordinationError::Pending.into());
            }
        };
        let raw = handle
            .hydrate_path_at_heads::<am_utils_rs::codecs::ThroughJson<serde_json::Value>>(
                &pinned.heads.0,
                automerge::ROOT,
                vec![
                    "facets".into(),
                    autosurgeon::Prop::Key(reference.facet_key.to_string().into()),
                ],
            )
            .await?
            .ok_or_else(|| ferr!("pinned pool key facet unavailable"))?;
        let WellKnownFacet::Jwk(jwk) = WellKnownFacet::from_json(raw.0, WellKnownFacetTag::Jwk)?
        else {
            unreachable!("JWK decoder returns JWK")
        };
        serde_json::from_value::<JwkOct>(serde_json::to_value(jwk)?)?.to_master_key()
    }

    fn check_binding(&self, representation: &Representation) -> Res<()> {
        ensure!(
            representation.original.key.scope == self.scope
                && representation.binding.incarnation == self.incarnation,
            "register transport binding mismatch"
        );
        let key_ref = Url::parse(std::str::from_utf8(&representation.binding.key_ref)?)?;
        ensure!(
            self.sources
                .read()
                .expect("register source lock poisoned")
                .allowed_key_refs
                .contains(&key_ref),
            "key reference outside configured domain slots"
        );
        ensure!(
            !representation.binding.key_heads.is_empty(),
            "key reference pins no heads"
        );
        EncodingParams::from_encoding_parameters(
            std::str::from_utf8(&representation.binding.encoding)?,
            &serde_json::from_slice(&representation.binding.parameters)?,
        )?;
        Ok(())
    }

    async fn fresh_authority(&self, required: Access) -> Res<CoordinationAuthority> {
        let sampled = self
            .repo
            .coordination_authority(self.authority.document().clone(), *self.authority.group())
            .await
            .wrap_err_with(|| {
                format!(
                    "sampling {required:?} coordination authority for {}",
                    self.authority.document()
                )
            })?;
        self.repo
            .admit_coordination_access(&sampled, required)
            .await
            .wrap_err_with(|| {
                format!(
                    "admitting {required:?} coordination authority for {}",
                    self.authority.document()
                )
            })
    }

    /// Checked local publisher identity; no signing key escapes this boundary.
    pub async fn local_writer(&self) -> Res<[u8; 32]> {
        let authority = self.fresh_authority(Access::Edit).await?;
        Ok(self
            .repo
            .with_coordination_signer(&authority, |signer| Ok(signer.verifying_key().to_bytes()))
            .await?)
    }

    async fn reserve_sequence(&self, writer: [u8; 32]) -> Res<u64> {
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        self.check_current(&mut write).await?;
        let next: i64 = sqlx::query_scalar(
            "INSERT INTO encrypted_register_sequence(
                register_scope
              , writer
              , next_sequence
            ) VALUES (?, ?, 2)
            ON CONFLICT(register_scope, writer)
            DO UPDATE SET next_sequence = next_sequence + 1
            RETURNING next_sequence - 1",
        )
        .bind(&self.scope)
        .bind(writer.as_slice())
        .fetch_one(&mut **write.context_mut())
        .await?;
        ensure!(next > 0, "coordination writer sequence exhausted");
        write.commit().await?;
        Ok(u64::try_from(next).expect("positive writer sequence"))
    }

    /// Reserves sequence before signing; interrupted/failed publications leave gaps, never reuse.
    /// Frontier is explicit domain-owned causal knowledge, not an inferred historical DAG.
    pub async fn publish_local(
        &self,
        slot: &[u8],
        frontier: Vec<CausalVersion>,
        body: Vec<u8>,
    ) -> Res<MergeOutcome> {
        ensure!(
            body.len() <= MAX_CIPHERTEXT_BYTES / 2,
            "coordination body exceeds budget"
        );
        ensure!(
            frontier.len() <= MAX_FRONTIER_ENTRIES,
            "coordination frontier exceeds budget"
        );
        let key = self.key(slot)?;
        let authority = self.fresh_authority(Access::Edit).await?;
        let writer = self
            .repo
            .with_coordination_signer(&authority, |signer| Ok(signer.verifying_key().to_bytes()))
            .await?;
        let sequence = self.reserve_sequence(writer).await?;
        let authority = self.fresh_authority(Access::Edit).await?;
        let (statement, header) = self
            .repo
            .with_coordination_signer(&authority, |signer| {
                assert_eq!(
                    signer.verifying_key().to_bytes(),
                    writer,
                    "reserved local signer identity is stable"
                );
                OriginalStatement::sign(
                    key.clone(),
                    sequence,
                    frontier,
                    body,
                    signer.verifying_key(),
                    signer,
                )
                .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
            })
            .await?;
        let original = serde_json::to_vec(&statement)?;
        // A refresh waits for this publication to commit; one ciphertext never
        // mixes key bytes with another pinned-key header.
        let publication_key = self.publication_key.read().await;
        let master = self.resolve_key(&publication_key).await?;
        let framing = EncodingParams::DEFAULT;
        let ciphertext = encrypt_with_rs(&master, &original, framing.record_size, framing.padding);
        let mut key_heads: Vec<_> = publication_key.heads.0.iter().map(|head| head.0).collect();
        key_heads.sort_unstable();
        key_heads.dedup();
        let binding = RepresentationBinding {
            incarnation: self.incarnation,
            key_ref: publication_key.key_ref.as_str().as_bytes().to_vec(),
            key_heads,
            encoding: CONTENT_ENCODING_AES128GCM.as_bytes().to_vec(),
            parameters: serde_json::to_vec(&framing)?,
        };
        let authority = self.fresh_authority(Access::Edit).await?;
        let representation = self
            .repo
            .with_coordination_signer(&authority, |signer| {
                Representation::sign(header, binding, ciphertext, signer.verifying_key(), signer)
                    .map_err(|error| big_repo::CoordinationError::Other(ferr!(error)))
            })
            .await?;
        self.merge_and_commit(Incoming::Local(representation)).await
    }

    /// Backend receive boundary. Hints are not grants: only this store's bound part is adopted.
    /// Rejected evidence never reaches publication's object/membership allocation.
    pub async fn receive(
        &self,
        obj_id: &ObjKey,
        part_hints: &[PartKey],
        payload: serde_json::Value,
    ) -> Res<MergeOutcome> {
        let incoming = self.prepare_receive(obj_id, part_hints, payload)?;
        self.receive_prepared(incoming).await
    }

    pub(crate) fn prepare_receive(
        &self,
        obj_id: &ObjKey,
        part_hints: &[PartKey],
        payload: serde_json::Value,
    ) -> Res<RegisterSnapshot> {
        ensure!(
            !part_hints.is_empty() && part_hints.iter().all(|part| self.accepts_part(part)),
            "coordination part binding mismatch"
        );
        let incoming: RegisterSnapshot = serde_json::from_value(payload)?;
        ensure!(
            incoming.key.encode().as_slice() == obj_id.as_bytes(),
            "coordination object binding mismatch"
        );
        ensure!(
            incoming.key.scope == self.scope && incoming.key.slot.len() <= MAX_SLOT_BYTES,
            "coordination logical binding mismatch"
        );
        Ok(incoming)
    }

    pub(crate) async fn receive_prepared(&self, incoming: RegisterSnapshot) -> Res<MergeOutcome> {
        self.fresh_authority(Access::Relay).await?;
        self.merge_and_commit(Incoming::Remote(incoming)).await
    }

    async fn check_current(&self, write: &mut SqliteObjWrite<'_>) -> Res<()> {
        let selected: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM encrypted_register_rotation
                            WHERE register_scope = ?
                              AND part_id = ?
                              AND authority_document = ?)",
        )
        .bind(&self.scope)
        .bind(self.part.as_bytes())
        .bind(self.authority.document().to_string())
        .fetch_one(&mut **write.context_mut())
        .await?;
        if !selected {
            return Err(CoordinationError::Unauthorized.into());
        }
        Ok(())
    }

    async fn merge_and_commit(&self, incoming: Incoming) -> Res<MergeOutcome> {
        let key = match &incoming {
            Incoming::Local(representation) => &representation.original.key,
            Incoming::Remote(snapshot) => &snapshot.key,
        }
        .clone();
        match &incoming {
            Incoming::Local(value) => self.check_binding(value)?,
            Incoming::Remote(snapshot) => {
                for lane in snapshot.lanes.values() {
                    if let LaneState::Current { representation } = lane {
                        self.check_binding(representation)?;
                    }
                }
            }
        }
        let encoded = key.encode();
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(encoded.clone()))
            .await?;
        let scope = write.scope_id();
        self.check_current(&mut write).await?;
        let current: Option<String> = sqlx::query_scalar(
            "SELECT snapshot_json FROM encrypted_register_current WHERE scope_id = ? AND logical_key = ?"
        ).bind(scope).bind(&encoded).fetch_optional(&mut **write.context_mut()).await?;
        let limits = self.limits(key);
        let mut state = match current {
            Some(current) => EncryptedRegister::restore(limits, serde_json::from_str(&current)?)?,
            None => EncryptedRegister::new(limits),
        };
        let outcome = match incoming {
            Incoming::Local(representation) => state.merge_local(representation)?,
            Incoming::Remote(snapshot) => state.merge_snapshot(snapshot)?,
        };
        let is_member: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1
                  FROM big_sync_members m
                  JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
                  JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
                 WHERE m.scope_id = ? AND p.part_id = ? AND o.obj_id = ? AND m.event_type != 2
            )",
        )
        .bind(scope)
        .bind(self.part.as_bytes())
        .bind(&encoded)
        .fetch_one(&mut **write.context_mut())
        .await?;
        if outcome != MergeOutcome::Unchanged || !is_member {
            let payload = serde_json::to_value(state.snapshot())?;
            let json = serde_json::to_string(&payload)?;
            sqlx::query(
                "INSERT INTO encrypted_register_current(
                    scope_id
                  , logical_key
                  , snapshot_json
                ) VALUES (?, ?, ?)
                ON CONFLICT(scope_id, logical_key)
                DO UPDATE SET snapshot_json = excluded.snapshot_json",
            )
            .bind(scope)
            .bind(&encoded)
            .bind(json)
            .execute(&mut **write.context_mut())
            .await?;
            write
                .publish(payload, std::slice::from_ref(&self.part))
                .await?;
        }
        write.commit().await?;
        Ok(outcome)
    }

    /// Indexed opaque current lookup, permitted for configured encrypted relays.
    pub async fn current(&self, slot: &[u8]) -> Res<Option<RegisterSnapshot>> {
        self.fresh_authority(Access::Relay).await?;
        let key = self.key(slot)?;
        let mut write = self
            .parts
            .begin_obj_write(ObjKey::new(key.encode()))
            .await?;
        let scope = write.scope_id();
        let json: Option<String> = sqlx::query_scalar(
            "SELECT snapshot_json FROM encrypted_register_current WHERE scope_id = ? AND logical_key = ?"
        ).bind(scope).bind(key.encode()).fetch_optional(&mut **write.context_mut()).await?;
        write.commit().await?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    /// Decrypt then authenticate the exact producer statement; plaintext is never persisted here.
    pub async fn open_original(&self, representation: &Representation) -> Res<OriginalStatement> {
        self.fresh_authority(Access::Read)
            .await
            .wrap_err("admitting original ciphertext before decryption")?;
        self.check_binding(representation)?;
        EncryptedRegister::new(self.limits(representation.original.key.clone()))
            .verify_representation(representation)?;
        let pinned = PinnedJwk {
            key_ref: Url::parse(std::str::from_utf8(&representation.binding.key_ref)?)?,
            heads: ChangeHashSet(
                representation
                    .binding
                    .key_heads
                    .iter()
                    .copied()
                    .map(automerge::ChangeHash)
                    .collect::<Vec<_>>()
                    .into(),
            ),
        };
        let master = self.resolve_key(&pinned).await?;
        let framing = EncodingParams::from_encoding_parameters(
            std::str::from_utf8(&representation.binding.encoding)?,
            &serde_json::from_slice(&representation.binding.parameters)?,
        )?;
        ensure!(
            representation.ciphertext.len() >= 21,
            "truncated RFC 8188 header"
        );
        let wire_rs = u32::from_be_bytes(
            representation.ciphertext[16..20]
                .try_into()
                .expect("four-byte record size"),
        );
        ensure!(
            u64::from(wire_rs) == framing.record_size,
            "authenticated framing differs from ciphertext header"
        );
        let bytes = decrypt_bytes(&master, &representation.ciphertext)?;
        let statement: OriginalStatement = serde_json::from_slice(&bytes)?;
        statement.verify_header(&representation.original)?;
        // Key materialization awaits may race group revocation. Admit Read again
        // at plaintext release, not only before resolving the pinned native key.
        self.fresh_authority(Access::Read)
            .await
            .wrap_err("admitting original plaintext release after key materialization")?;
        let source = daybook_types::url::parse_facet_ref(&pinned.key_ref)?;
        let document: big_repo::DocumentId = source.doc_id.as_str().parse()?;
        if document != *self.authority.document() {
            let group = self
                .source_group(source.doc_id.as_str())
                .expect("validated historical key authority");
            let authority = self.repo.coordination_authority(document, group).await?;
            self.repo.admit_coordination_read(&authority).await?;
        }
        Ok(statement)
    }

    pub async fn consumer_checkpoint(&self, consumer: &[u8]) -> Res<CursorIndex> {
        self.fresh_authority(Access::Read)
            .await
            .wrap_err("admitting consumer checkpoint read")?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        let scope = write.scope_id();
        self.check_current(&mut write).await?;
        let row: Option<i64> = sqlx::query_scalar(
            "SELECT through_revision
               FROM encrypted_register_consumer_v2
              WHERE scope_id = ? AND consumer = ? AND selector = ?",
        )
        .bind(scope)
        .bind(consumer)
        .bind(self.part.as_bytes())
        .fetch_optional(&mut **write.context_mut())
        .await?;
        let through =
            match row {
                Some(through) => through,
                None => sqlx::query_scalar(
                    "SELECT replay_after FROM encrypted_register_rotation WHERE register_scope = ?",
                )
                .bind(&self.scope)
                .fetch_one(&mut **write.context_mut())
                .await?,
            };
        write.commit().await?;
        Ok(u64::try_from(through)?)
    }

    /// Current part checkpoint + tail, not an append-only effect journal or boot corpus walk.
    pub async fn open_consumer_reader(
        &self,
        consumer: &[u8],
    ) -> Res<Box<dyn LocalPartRevisionReader>> {
        let after = self.consumer_checkpoint(consumer).await?;
        self.parts
            .open_revision_reader(SubPartsRequest {
                lower_bound: after,
                targets: [SubscriptionTarget::Part {
                    part_id: self.part.clone(),
                    cursor: after,
                }]
                .into(),
            })
            .await?
            .map_err(|error| ferr!("coordination revision reader: {error:?}"))
    }

    /// Apply derived SQL changes through context_mut; checkpoint and those changes commit together.
    pub async fn begin_consumer_settlement<'a>(
        &'a self,
        consumer: &[u8],
        through: CursorIndex,
    ) -> Res<ConsumerSettlement<'a>> {
        self.fresh_authority(Access::Read)
            .await
            .wrap_err("admitting consumer settlement")?;
        let mut write = self.parts.begin_obj_write(self.control_key()).await?;
        let scope = write.scope_id();
        self.check_current(&mut write).await?;
        let row: Option<i64> = sqlx::query_scalar(
            "SELECT through_revision
               FROM encrypted_register_consumer_v2
              WHERE scope_id = ? AND consumer = ? AND selector = ?",
        )
        .bind(scope)
        .bind(consumer)
        .bind(self.part.as_bytes())
        .fetch_optional(&mut **write.context_mut())
        .await?;
        let through = i64::try_from(through)?;
        if let Some(previous) = row {
            ensure!(
                through >= previous,
                "coordination consumer cursor regressed"
            );
        }
        // Read the fence through the owned writer transaction. Acquiring a
        // separate reader here can invert pool ownership under concurrent sync.
        let latest: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&mut **write.context_mut())
                .await?;
        ensure!(
            through <= latest,
            "coordination consumer cursor exceeds committed revision"
        );
        sqlx::query(
            "INSERT INTO encrypted_register_consumer_v2(
                scope_id
              , consumer
              , selector
              , through_revision
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(scope_id, consumer, selector)
            DO UPDATE SET through_revision = excluded.through_revision",
        )
        .bind(scope)
        .bind(consumer)
        .bind(self.part.as_bytes())
        .bind(through)
        .execute(&mut **write.context_mut())
        .await?;
        Ok(ConsumerSettlement { write })
    }
}

/// Concrete SQLite settlement: drop rolls back the cursor and derived changes together.
pub struct ConsumerSettlement<'a> {
    write: SqliteObjWrite<'a>,
}

impl<'a> ConsumerSettlement<'a> {
    pub fn context_mut(&mut self) -> &mut Transaction<'a, Sqlite> {
        self.write.context_mut()
    }
    pub async fn commit(self) -> Res<()> {
        self.write.commit().await
    }
}

#[cfg(test)]
mod tests;
